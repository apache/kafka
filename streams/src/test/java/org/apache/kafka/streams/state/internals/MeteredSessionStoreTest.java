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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
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
public class MeteredSessionStoreTest {

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
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final long START_TIMESTAMP = 24L;
    private static final long END_TIMESTAMP = 42L;
    private static final int RETENTION_PERIOD = 100;

    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    private final Metrics metrics = new Metrics();
    private MeteredSessionStore<String, String> store;
    @Mock
    private SessionStore<Bytes, byte[]> innerStore;
    @Mock
    private InternalProcessorContext context;

    private Map<String, String> tags;
    
    @Before
    public void before() {
        final Time mockTime = new MockTime();
        store = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            mockTime
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.metrics())
            .thenReturn(new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST, mockTime));
        when(context.taskId()).thenReturn(taskId);
        when(context.changelogFor(STORE_NAME)).thenReturn(CHANGELOG_TOPIC);
        when(innerStore.name()).thenReturn(STORE_NAME);
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    private void init() {
        store.init((StateStoreContext) context, store);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final MeteredSessionStore<String, String> outer = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
        doNothing().when(innerStore).init((ProcessorContext) context, outer);
        outer.init((ProcessorContext) context, outer);
    }

    @Test
    public void shouldDelegateInit() {
        final MeteredSessionStore<String, String> outer = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
        doNothing().when(innerStore).init((StateStoreContext) context, outer);
        outer.init((StateStoreContext) context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME, taskId.topologyName());
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
        when(innerStore.fetchSession(KEY_BYTES, START_TIMESTAMP, END_TIMESTAMP)).thenReturn(VALUE_BYTES);
        store = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            keySerde,
            valueSerde,
            new MockTime()
        );
        store.init((StateStoreContext) context, store);

        store.fetchSession(KEY, START_TIMESTAMP, END_TIMESTAMP);
        store.put(WINDOWED_KEY, VALUE);
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
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        doNothing().when(innerStore).put(WINDOWED_KEY_BYTES, VALUE_BYTES);
        init();

        store.put(WINDOWED_KEY, VALUE);

        // it suffices to verify one put metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("put-rate");
        assertTrue(((Double) metric.metricValue()) > 0);
    }

    @Test
    public void shouldFindSessionsFromStoreAndRecordFetchMetric() {
        when(innerStore.findSessions(KEY_BYTES, 0, 0))
                .thenReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFindSessionsFromStoreAndRecordFetchMetric() {
        when(innerStore.backwardFindSessions(KEY_BYTES, 0, 0))
            .thenReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        when(innerStore.findSessions(KEY_BYTES, KEY_BYTES, 0, 0))
                .thenReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFindSessionRangeFromStoreAndRecordFetchMetric() {
        when(innerStore.backwardFindSessions(KEY_BYTES, KEY_BYTES, 0, 0))
            .thenReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all put metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldRemoveFromStoreAndRecordRemoveMetric() {
        doNothing().when(innerStore).remove(WINDOWED_KEY_BYTES);

        init();

        store.remove(new Windowed<>(KEY, new SessionWindow(0, 0)));

        // it suffices to verify one remove metric since all remove metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("remove-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFetchForKeyAndRecordFetchMetric() {
        when(innerStore.fetch(KEY_BYTES))
                .thenReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.fetch(KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFetchForKeyAndRecordFetchMetric() {
        when(innerStore.backwardFetch(KEY_BYTES))
            .thenReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFetch(KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFetchRangeFromStoreAndRecordFetchMetric() {
        when(innerStore.fetch(KEY_BYTES, KEY_BYTES))
                .thenReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.fetch(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldBackwardFetchRangeFromStoreAndRecordFetchMetric() {
        when(innerStore.backwardFetch(KEY_BYTES, KEY_BYTES))
            .thenReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFetch(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        // it suffices to verify one fetch metric since all fetch metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldReturnNoSessionsWhenFetchedKeyHasExpired() {
        final long systemTime = Time.SYSTEM.milliseconds();
        when(innerStore.findSessions(KEY_BYTES, systemTime - RETENTION_PERIOD, systemTime))
                .thenReturn(new KeyValueIteratorStub<>(KeyValueIterators.emptyIterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, systemTime - RETENTION_PERIOD, systemTime);
        assertFalse(iterator.hasNext());
        iterator.close();
    }

    @Test
    public void shouldReturnNoSessionsInBackwardOrderWhenFetchedKeyHasExpired() {
        final long systemTime = Time.SYSTEM.milliseconds();
        when(innerStore.backwardFindSessions(KEY_BYTES, systemTime - RETENTION_PERIOD, systemTime))
                .thenReturn(new KeyValueIteratorStub<>(KeyValueIterators.emptyIterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, systemTime - RETENTION_PERIOD, systemTime);
        assertFalse(iterator.hasNext());
        iterator.close();
    }

    @Test
    public void shouldNotFindExpiredSessionRangeFromStore() {
        final long systemTime = Time.SYSTEM.milliseconds();
        when(innerStore.findSessions(KEY_BYTES, KEY_BYTES, systemTime - RETENTION_PERIOD, systemTime))
                .thenReturn(new KeyValueIteratorStub<>(KeyValueIterators.emptyIterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, KEY, systemTime - RETENTION_PERIOD, systemTime);
        assertFalse(iterator.hasNext());
        iterator.close();
    }

    @Test
    public void shouldNotFindExpiredSessionRangeInBackwardOrderFromStore() {
        final long systemTime = Time.SYSTEM.milliseconds();
        when(innerStore.backwardFindSessions(KEY_BYTES, KEY_BYTES, systemTime - RETENTION_PERIOD, systemTime))
                .thenReturn(new KeyValueIteratorStub<>(KeyValueIterators.emptyIterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, KEY, systemTime - RETENTION_PERIOD, systemTime);
        assertFalse(iterator.hasNext());
        iterator.close();
    }

    @Test
    public void shouldRecordRestoreTimeOnInit() {
        init();

        // it suffices to verify one restore metric since all restore metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("restore-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull() {
        when(innerStore.fetchSession(Bytes.wrap("a".getBytes()), 0, Long.MAX_VALUE)).thenReturn(null);

        init();
        assertNull(store.fetchSession("a", 0, Long.MAX_VALUE));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.put(null, "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.remove(null));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfWrappedKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.put(new Windowed<>(null, new SessionWindow(0, 0)), "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfWrappedKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.remove(new Windowed<>(null, new SessionWindow(0, 0))));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfWindowIsNull() {
        assertThrows(NullPointerException.class, () -> store.put(new Windowed<>(KEY, null), "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfWindowIsNull() {
        assertThrows(NullPointerException.class, () -> store.remove(new Windowed<>(KEY, null)));
    }

    @Test
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.fetch(null));
    }

    @Test
    public void shouldThrowNullPointerOnFetchSessionIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.fetchSession(null, 0, Long.MAX_VALUE));
    }

    @Test
    public void shouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> store.fetch(null, "to"));
    }

    @Test
    public void shouldThrowNullPointerOnFetchRangeIfToIsNull() {
        assertThrows(NullPointerException.class, () -> store.fetch("from", null));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFetch(null));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFetch(null, "to"));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfToIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFetch("from", null));
    }

    @Test
    public void shouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.findSessions(null, 0, 0));
    }

    @Test
    public void shouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> store.findSessions(null, "a", 0, 0));
    }

    @Test
    public void shouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        assertThrows(NullPointerException.class, () -> store.findSessions("a", null, 0, 0));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions(null, 0, 0));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsRangeIfFromIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions(null, "a", 0, 0));
    }

    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsRangeIfToIsNull() {
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions("a", null, 0, 0));
    }

    private interface CachedSessionStore extends SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedSessionStore cachedSessionStore = mock(CachedSessionStore.class);

        when(cachedSessionStore.setFlushListener(any(CacheFlushListener.class), eq(false))).thenReturn(true);

        store = new MeteredSessionStore<>(
            cachedSessionStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
        assertTrue(store.setFlushListener(null, false));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(store.setFlushListener(null, false));
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        doNothing().when(innerStore).close();
        init(); // replays "inner"

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        store.close();
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        doThrow(new RuntimeException("Oops!")).when(innerStore).close();
        init(); // replays "inner"

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
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
}
