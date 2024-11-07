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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredVersionedKeyValueStoreTest {

    private static final String STORE_NAME = "versioned_store";
    private static final Serde<String> STRING_SERDE = new StringSerde();
    private static final Serde<ValueAndTimestamp<String>> VALUE_AND_TIMESTAMP_SERDE = new ValueAndTimestampSerde<>(STRING_SERDE);
    private static final String METRICS_SCOPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String APPLICATION_ID = "test-app";
    private static final TaskId TASK_ID = new TaskId(0, 0, "My-Topology");

    private static final String KEY = "k";
    private static final String VALUE = "v";
    private static final long TIMESTAMP = 10L;
    private static final Bytes RAW_KEY = new Bytes(STRING_SERDE.serializer().serialize(null, KEY));
    private static final byte[] RAW_VALUE = STRING_SERDE.serializer().serialize(null, VALUE);
    private static final byte[] RAW_VALUE_AND_TIMESTAMP = VALUE_AND_TIMESTAMP_SERDE.serializer()
        .serialize(null, ValueAndTimestamp.make(VALUE, TIMESTAMP));

    private final VersionedBytesStore inner = mock(VersionedBytesStore.class);
    private final Metrics metrics = new Metrics();
    private final Time mockTime = new MockTime();
    private final String threadId = Thread.currentThread().getName();
    private final InternalProcessorContext context = mock(InternalProcessorContext.class);
    private Map<String, String> tags;

    private MeteredVersionedKeyValueStore<String, String> store;

    @BeforeEach
    public void setUp() {
        when(inner.name()).thenReturn(STORE_NAME);
        when(context.metrics()).thenReturn(new StreamsMetricsImpl(metrics, "test", "processId", mockTime));
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
            STRING_SERDE
        );
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
            valueSerde
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
        when(inner.put(RAW_KEY, RAW_VALUE, TIMESTAMP)).thenReturn(PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        final long validto = store.put(KEY, VALUE, TIMESTAMP);

        assertThat(validto, is(PUT_RETURN_CODE_VALID_TO_UNDEFINED));
        assertThat((Double) getMetric("put-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnDelete() {
        when(inner.delete(RAW_KEY, TIMESTAMP)).thenReturn(RAW_VALUE_AND_TIMESTAMP);

        final VersionedRecord<String> result = store.delete(KEY, TIMESTAMP);

        assertThat(result, is(new VersionedRecord<>(VALUE, TIMESTAMP)));
        assertThat((Double) getMetric("delete-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnGet() {
        when(inner.get(RAW_KEY)).thenReturn(RAW_VALUE_AND_TIMESTAMP);

        final VersionedRecord<String> result = store.get(KEY);

        assertThat(result, is(new VersionedRecord<>(VALUE, TIMESTAMP)));
        assertThat((Double) getMetric("get-rate").metricValue(), greaterThan(0.0));
    }

    @Test
    public void shouldDelegateAndRecordMetricsOnGetWithTimestamp() {
        when(inner.get(RAW_KEY, TIMESTAMP)).thenReturn(RAW_VALUE_AND_TIMESTAMP);

        final VersionedRecord<String> result = store.get(KEY, TIMESTAMP);

        assertThat(result, is(new VersionedRecord<>(VALUE, TIMESTAMP)));
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

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowOnIQv2RangeQuery() {
        assertThrows(UnsupportedOperationException.class, () -> store.query(mock(RangeQuery.class), null, null));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowOnIQv2KeyQuery() {
        assertThrows(UnsupportedOperationException.class, () -> store.query(mock(KeyQuery.class), null, null));
    }

    @Test
    public void shouldThrowOnMultiVersionedKeyQueryInvalidTimeRange() {
        MultiVersionedKeyQuery<String, Object> query = MultiVersionedKeyQuery.withKey("key");
        final Instant fromTime = Instant.now();
        final Instant toTime = Instant.ofEpochMilli(fromTime.toEpochMilli() - 100);
        query = query.fromTime(fromTime).toTime(toTime);
        final MultiVersionedKeyQuery<String, Object> finalQuery = query;
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> store.query(finalQuery, null, null));
        assertEquals("The `fromTime` timestamp must be smaller than the `toTime` timestamp.", exception.getMessage());
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldDelegateAndAddExecutionInfoOnCustomQuery() {
        final Query query = mock(Query.class);
        final PositionBound positionBound = mock(PositionBound.class);
        final QueryConfig queryConfig = mock(QueryConfig.class);
        final QueryResult result = mock(QueryResult.class);
        when(inner.query(query, positionBound, queryConfig)).thenReturn(result);
        when(queryConfig.isCollectExecutionInfo()).thenReturn(true);

        assertThat(store.query(query, positionBound, queryConfig), is(result));
        verify(result).addExecutionInfo(anyString());
    }

    @Test
    public void shouldDelegateName() {
        when(inner.name()).thenReturn(STORE_NAME);

        assertThat(store.name(), is(STORE_NAME));
    }

    @Test
    public void shouldDelegatePersistent() {
        // `persistent = true` case
        when(inner.persistent()).thenReturn(true);
        assertThat(store.persistent(), is(true));

        // `persistent = false` case
        when(inner.persistent()).thenReturn(false);
        assertThat(store.persistent(), is(false));
    }

    @Test
    public void shouldDelegateIsOpen() {
        // `isOpen = true` case
        when(inner.isOpen()).thenReturn(true);
        assertThat(store.isOpen(), is(true));

        // `isOpen = false` case
        when(inner.isOpen()).thenReturn(false);
        assertThat(store.isOpen(), is(false));
    }

    @Test
    public void shouldDelegateGetPosition() {
        final Position position = mock(Position.class);
        when(inner.getPosition()).thenReturn(position);

        assertThat(store.getPosition(), is(position));
    }

    @Test
    public void shouldTrackOpenIteratorsMetric() {
        final MultiVersionedKeyQuery<String, String> query = MultiVersionedKeyQuery.withKey(KEY);
        final PositionBound bound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);
        when(inner.query(any(), any(), any())).thenReturn(
            QueryResult.forResult(new LogicalSegmentIterator(Collections.emptyListIterator(), RAW_KEY, 0L, 0L, ResultOrder.ANY)));

        final KafkaMetric openIteratorsMetric = getMetric("num-open-iterators");
        assertThat(openIteratorsMetric, not(nullValue()));

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));

        final QueryResult<VersionedRecordIterator<String>> result = store.query(query, bound, config);

        try (final VersionedRecordIterator<String> iterator = result.getResult()) {
            assertThat((Long) openIteratorsMetric.metricValue(), equalTo(1L));
        }

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));
    }

    @Test
    public void shouldTimeIteratorDuration() {
        final MultiVersionedKeyQuery<String, String> query = MultiVersionedKeyQuery.withKey(KEY);
        final PositionBound bound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);
        when(inner.query(any(), any(), any())).thenReturn(
                QueryResult.forResult(new LogicalSegmentIterator(Collections.emptyListIterator(), RAW_KEY, 0L, 0L, ResultOrder.ANY)));

        final KafkaMetric iteratorDurationAvgMetric = getMetric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMaxMetric = getMetric("iterator-duration-max");
        assertThat(iteratorDurationAvgMetric, not(nullValue()));
        assertThat(iteratorDurationMaxMetric, not(nullValue()));

        assertThat((Double) iteratorDurationAvgMetric.metricValue(), equalTo(Double.NaN));
        assertThat((Double) iteratorDurationMaxMetric.metricValue(), equalTo(Double.NaN));

        final QueryResult<VersionedRecordIterator<String>> first = store.query(query, bound, config);
        try (final VersionedRecordIterator<String> iterator = first.getResult()) {
            // nothing to do, just close immediately
            mockTime.sleep(2);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));

        final QueryResult<VersionedRecordIterator<String>> second = store.query(query, bound, config);
        try (final VersionedRecordIterator<String> iterator = second.getResult()) {
            // nothing to do, just close immediately
            mockTime.sleep(3);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.5 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(3.0 * TimeUnit.MILLISECONDS.toNanos(1)));
    }

    @Test
    public void shouldTrackOldestOpenIteratorTimestamp() {
        final MultiVersionedKeyQuery<String, String> query = MultiVersionedKeyQuery.withKey(KEY);
        final PositionBound bound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);
        when(inner.query(any(), any(), any())).thenReturn(
                QueryResult.forResult(new LogicalSegmentIterator(Collections.emptyListIterator(), RAW_KEY, 0L, 0L, ResultOrder.ANY)));

        final KafkaMetric oldestIteratorTimestampMetric = getMetric("oldest-iterator-open-since-ms");
        assertThat(oldestIteratorTimestampMetric, not(nullValue()));

        assertThat(oldestIteratorTimestampMetric.metricValue(), nullValue());

        final QueryResult<VersionedRecordIterator<String>> first = store.query(query, bound, config);
        VersionedRecordIterator<String> secondIterator = null;
        final long secondTime;
        try {
            try (final VersionedRecordIterator<String> iterator = first.getResult()) {
                final long oldestTimestamp = mockTime.milliseconds();
                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);

                // open a second iterator before closing the first to test that we still produce the first iterator's timestamp
                final QueryResult<VersionedRecordIterator<String>> second = store.query(query, bound, config);
                secondIterator = second.getResult();
                secondTime = mockTime.milliseconds();

                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);
            }

            // now that the first iterator is closed, check that the timestamp has advanced to the still open second iterator
            assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(secondTime));
        } finally {
            if (secondIterator != null) {
                secondIterator.close();
            }
        }

        assertThat((Integer) oldestIteratorTimestampMetric.metricValue(), nullValue());
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