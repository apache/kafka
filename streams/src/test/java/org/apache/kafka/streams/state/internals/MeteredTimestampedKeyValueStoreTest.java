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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore.RawAndDeserializedValue;
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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredTimestampedKeyValueStoreTest {

    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_NAME = "store-name";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String CHANGELOG_TOPIC = "changelog-topic-name";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final ValueAndTimestamp<String> VALUE_AND_TIMESTAMP =
        ValueAndTimestamp.make("value", 97L);
    // timestamp is 97 what is ASCII of 'a'
    private static final byte[] VALUE_AND_TIMESTAMP_BYTES = "\0\0\0\0\0\0\0avalue".getBytes();


    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    @Mock
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock
    private InternalProcessorContext<?, ?> context;
    private MockTime mockTime;

    private static final Map<String, Object> CONFIGS =  mkMap(mkEntry(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, APPLICATION_ID));

    private MeteredTimestampedKeyValueStore<String, String> metered;
    private final KeyValue<Bytes, byte[]> byteKeyValueTimestampPair = KeyValue.pair(KEY_BYTES,
        VALUE_AND_TIMESTAMP_BYTES
    );
    private final Metrics metrics = new Metrics();
    private Map<String, String> tags;

    private void setUpWithoutContext() {
        mockTime = new MockTime();
        metered = new MeteredTimestampedKeyValueStore<>(
                inner,
                "scope",
                mockTime,
                Serdes.String(),
                new ValueAndTimestampSerde<>(Serdes.String())
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
            .thenReturn(new StreamsMetricsImpl(metrics, "test", "processId", mockTime));
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
        final MeteredTimestampedKeyValueStore<String, String> outer = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
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

    @SuppressWarnings("unchecked")
    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<ValueAndTimestamp<String>> valueSerde = mock(Serde.class);
        final Deserializer<ValueAndTimestamp<String>> valueDeserializer = mock(Deserializer.class);
        final Serializer<ValueAndTimestamp<String>> valueSerializer = mock(Serializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        when(keySerializer.serialize(topic, KEY)).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        when(valueDeserializer.deserialize(topic, VALUE_AND_TIMESTAMP_BYTES)).thenReturn(VALUE_AND_TIMESTAMP);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        when(valueSerializer.serialize(topic, VALUE_AND_TIMESTAMP)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        when(inner.get(KEY_BYTES)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        metered = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        metered.init(context, metered);

        metered.get(KEY);
        metered.put(KEY, VALUE_AND_TIMESTAMP);
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
        doNothing().when(inner).put(KEY_BYTES, VALUE_AND_TIMESTAMP_BYTES);
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetWithBinary() {
        setUp();
        when(inner.get(KEY_BYTES)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);

        init();

        final RawAndDeserializedValue<String> valueWithBinary = metered.getWithBinary(KEY);
        assertEquals(VALUE_AND_TIMESTAMP, valueWithBinary.value);
        assertArrayEquals(VALUE_AND_TIMESTAMP_BYTES, valueWithBinary.serializedValue);
    }

    @Test
    public void shouldNotPutIfSameValuesAndGreaterTimestamp() {
        setUp();
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);
        final ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String());
        final byte[] encodedOldValue = stringSerde.serializer().serialize("TOPIC", VALUE_AND_TIMESTAMP);

        final ValueAndTimestamp<String> newValueAndTimestamp = ValueAndTimestamp.make("value", 98L);
        assertFalse(metered.putIfDifferentValues(KEY, newValueAndTimestamp, encodedOldValue));
    }

    @Test
    public void shouldPutIfOutOfOrder() {
        setUp();
        doNothing().when(inner).put(KEY_BYTES, VALUE_AND_TIMESTAMP_BYTES);
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);

        final ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String());
        final byte[] encodedOldValue = stringSerde.serializer().serialize("TOPIC", VALUE_AND_TIMESTAMP);

        final ValueAndTimestamp<String> outOfOrderValueAndTimestamp = ValueAndTimestamp.make("value", 95L);
        assertTrue(metered.putIfDifferentValues(KEY, outOfOrderValueAndTimestamp, encodedOldValue));
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        setUp();
        when(inner.get(KEY_BYTES)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        init();

        assertThat(metered.get(KEY), equalTo(VALUE_AND_TIMESTAMP));

        final KafkaMetric metric = metric("get-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        setUp();
        when(inner.putIfAbsent(KEY_BYTES, VALUE_AND_TIMESTAMP_BYTES)).thenReturn(null);
        init();

        metered.putIfAbsent(KEY, VALUE_AND_TIMESTAMP);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, STORE_LEVEL_GROUP, "", tags));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        setUp();
        doNothing().when(inner).putAll(any(List.class));
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(KEY, VALUE_AND_TIMESTAMP)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        setUp();
        when(inner.delete(KEY_BYTES)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        init();

        metered.delete(KEY);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        setUp();
        when(inner.range(KEY_BYTES, KEY_BYTES)).thenReturn(
            new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.range(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE_AND_TIMESTAMP));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("range-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        setUp();
        when(inner.all())
            .thenReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(VALUE_AND_TIMESTAMP));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric(new MetricName("all-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldFlushInnerWhenFlushTimeRecords() {
        setUp();
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
        setUpWithoutContext();
        final CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore.class);

        when(cachedKeyValueStore.setFlushListener(any(CacheFlushListener.class), eq(false))).thenReturn(true);

        metered = new MeteredTimestampedKeyValueStore<>(
            cachedKeyValueStore,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String()));
        assertTrue(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        setUpWithoutContext();
        assertFalse(metered.setFlushListener(null, false));
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        setUpWithExpectSerdes();
        final MeteredTimestampedKeyValueStore<String, Long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            null,
            null
        );
        store.init(context, inner);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                throw new AssertionError(
                    "Serdes are not correctly set from processor context.",
                    exception
                );
            } else {
                throw exception;
            }
        }
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        setUp();
        final MeteredTimestampedKeyValueStore<String, Long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        store.init(context, inner);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
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
        assertThat(openIteratorsMetric, not(nullValue()));

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));

        try (final KeyValueIterator<String, ValueAndTimestamp<String>> unused = metered.all()) {
            assertThat((Long) openIteratorsMetric.metricValue(), equalTo(1L));
        }

        assertThat((Long) openIteratorsMetric.metricValue(), equalTo(0L));
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldTimeIteratorDuration() {
        setUp();
        when(inner.all()).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric iteratorDurationAvgMetric = metric("iterator-duration-avg");
        final KafkaMetric iteratorDurationMaxMetric = metric("iterator-duration-max");
        assertThat(iteratorDurationAvgMetric, not(nullValue()));
        assertThat(iteratorDurationMaxMetric, not(nullValue()));

        assertThat((Double) iteratorDurationAvgMetric.metricValue(), equalTo(Double.NaN));
        assertThat((Double) iteratorDurationMaxMetric.metricValue(), equalTo(Double.NaN));

        try (final KeyValueIterator<String, ValueAndTimestamp<String>> unused = metered.all()) {
            // nothing to do, just close immediately
            mockTime.sleep(2);
        }

        assertThat((double) iteratorDurationAvgMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat((double) iteratorDurationMaxMetric.metricValue(), equalTo(2.0 * TimeUnit.MILLISECONDS.toNanos(1)));

        try (final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.all()) {
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
        when(inner.all()).thenReturn(KeyValueIterators.emptyIterator());
        init();

        final KafkaMetric oldestIteratorTimestampMetric = metric("oldest-iterator-open-since-ms");
        assertThat(oldestIteratorTimestampMetric, not(nullValue()));

        assertThat(oldestIteratorTimestampMetric.metricValue(), nullValue());

        KeyValueIterator<String, ValueAndTimestamp<String>> second = null;
        final long secondTimestamp;
        try {
            try (final KeyValueIterator<String, ValueAndTimestamp<String>> unused = metered.all()) {
                final long oldestTimestamp = mockTime.milliseconds();
                assertThat((Long) oldestIteratorTimestampMetric.metricValue(), equalTo(oldestTimestamp));
                mockTime.sleep(100);

                // open a second iterator before closing the first to test that we still produce the first iterator's timestamp
                second = metered.all();
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
}
