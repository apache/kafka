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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
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
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
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
    private MockTime mockTime;
    private MeteredSessionStore<String, String> store;
    @Mock
    private SessionStore<Bytes, byte[]> innerStore;
    @Mock
    private InternalProcessorContext<?, ?> context;

    private Map<String, String> tags;
    
    public void setUpWithoutContext() {
        mockTime = new MockTime();
        store = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            mockTime
        );
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    public void setUp() {
        setUpWithoutContext();
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        when(context.applicationId()).thenReturn(APPLICATION_ID);
        when(context.metrics())
                .thenReturn(new StreamsMetricsImpl(metrics, "test", "processId", mockTime));
        when(context.taskId()).thenReturn(taskId);
        when(context.changelogFor(STORE_NAME)).thenReturn(CHANGELOG_TOPIC);
        when(innerStore.name()).thenReturn(STORE_NAME);
    }

    private void init() {
        store.init(context, store);
    }

    @Test
    public void shouldDelegateInit() {
        setUp();
        final MeteredSessionStore<String, String> outer = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
        doNothing().when(innerStore).init(context, outer);
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
        store.init(context, store);

        store.fetchSession(KEY, START_TIMESTAMP, END_TIMESTAMP);
        store.put(WINDOWED_KEY, VALUE);
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
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
        setUp();
        init();

        // it suffices to verify one restore metric since all restore metrics are recorded by the same sensor
        // and the sensor is tested elsewhere
        final KafkaMetric metric = metric("restore-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull() {
        setUp();
        when(innerStore.fetchSession(Bytes.wrap("a".getBytes()), 0, Long.MAX_VALUE)).thenReturn(null);

        init();
        assertNull(store.fetchSession("a", 0, Long.MAX_VALUE));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.put(null, "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.remove(null));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfWrappedKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.put(new Windowed<>(null, new SessionWindow(0, 0)), "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfWrappedKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.remove(new Windowed<>(null, new SessionWindow(0, 0))));
    }

    @Test
    public void shouldThrowNullPointerOnPutIfWindowIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.put(new Windowed<>(KEY, null), "a"));
    }

    @Test
    public void shouldThrowNullPointerOnRemoveIfWindowIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.remove(new Windowed<>(KEY, null)));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.fetch(null));
    }

    @Test
    public void shouldThrowNullPointerOnFetchSessionIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.fetchSession(null, 0, Long.MAX_VALUE));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.fetch(null, "to"));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFetchRangeIfToIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.fetch("from", null));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFetch(null));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfFromIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFetch(null, "to"));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFetchIfToIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFetch("from", null));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.findSessions(null, 0, 0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.findSessions(null, "a", 0, 0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.findSessions("a", null, 0, 0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsIfKeyIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions(null, 0, 0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsRangeIfFromIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions(null, "a", 0, 0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerOnBackwardFindSessionsRangeIfToIsNull() {
        setUpWithoutContext();
        assertThrows(NullPointerException.class, () -> store.backwardFindSessions("a", null, 0, 0));
    }

    private interface CachedSessionStore extends SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        setUpWithoutContext();
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
        setUpWithoutContext();
        assertFalse(store.setFlushListener(null, false));
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        setUp();
        doNothing().when(innerStore).close();
        init(); // replays "inner"

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        store.close();
        assertThat(storeMetrics(), empty());
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        setUp();
        doThrow(new RuntimeException("Oops!")).when(innerStore).close();
        init(); // replays "inner"

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTrackOpenIteratorsMetric() {
        setUp();
        when(innerStore.backwardFetch(KEY_BYTES)).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric openIteratorsMetric = metric("num-open-iterators");
        assertThat(openIteratorsMetric, not(nullValue()));

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));

        try (final KeyValueIterator<Windowed<String>, String> unused = store.backwardFetch(KEY)) {
            assertThat((Long) openIteratorsMetric.metricValue(), equalTo(1L));
        }

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTimeIteratorDuration() {
        setUp();
        when(innerStore.backwardFetch(KEY_BYTES)).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric iteratorDurationAvgMetric = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMaxMetric = metric("iterator-duration-max");
        assertThat(iteratorDurationAvgMetric, not(nullValue()));
        assertThat(iteratorDurationMaxMetric, not(nullValue()));

        assertThat((Double) iteratorDurationAvgMetric.metricValue(), equalTo(Double.NaN));
        assertThat((Double) iteratorDurationMaxMetric.metricValue(), equalTo(Double.NaN));

        try (final KeyValueIterator<Windowed<String>, String> unused = store.backwardFetch(KEY)) {
            // nothing to do, just close immediately
            mockTime.sleep(2);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));

        try (final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFetch(KEY)) {
            // nothing to do, just close immediately
            mockTime.sleep(3);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.5 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(3.0 * TimeUnit.MILLISECONDS.toNanos(1)));
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTrackOldestOpenIteratorTimestamp() {
        setUp();
        when(innerStore.backwardFetch(KEY_BYTES)).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric oldestIteratorTimestampMetric = metric("oldest-iterator-open-since-ms");
        assertThat(oldestIteratorTimestampMetric, not(nullValue()));

        assertThat(oldestIteratorTimestampMetric.metricValue(), nullValue());

        KeyValueIterator<Windowed<String>, String> second = null;
        final long secondTimestamp;
        try {
            try (final KeyValueIterator<Windowed<String>, String> unused = store.backwardFetch(KEY)) {
                final long oldestTimestamp = mockTime.milliseconds();
                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);

                // open a second iterator before closing the first to test that we still produce the first iterator's timestamp
                second = store.backwardFetch(KEY);
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
