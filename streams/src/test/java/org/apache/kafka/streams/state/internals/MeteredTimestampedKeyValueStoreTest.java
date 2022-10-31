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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore.RawAndDeserializedValue;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
 
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MeteredTimestampedKeyValueStoreTest {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

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
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private final static Map<String, Object> CONFIGS =  mkMap(mkEntry(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, APPLICATION_ID));

    private MeteredTimestampedKeyValueStore<String, String> metered;
    private final KeyValue<Bytes, byte[]> byteKeyValueTimestampPair = KeyValue.pair(KEY_BYTES,
        VALUE_AND_TIMESTAMP_BYTES
    );
    private final Metrics metrics = new Metrics();
    private Map<String, String> tags;

    @Before
    public void before() {
        final Time mockTime = new MockTime();
        metered = new MeteredTimestampedKeyValueStore<>(
            inner,
            "scope",
            mockTime,
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        expect(context.applicationId()).andStubReturn(APPLICATION_ID);
        expect(context.metrics())
            .andStubReturn(new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST, mockTime));
        expect(context.taskId()).andStubReturn(taskId);
        expect(context.changelogFor(STORE_NAME)).andStubReturn(CHANGELOG_TOPIC);
        expectSerdes();
        expect(inner.name()).andStubReturn(STORE_NAME);
        expect(context.appConfigs()).andStubReturn(CONFIGS);
        tags = mkMap(
            mkEntry(THREAD_ID_TAG_KEY, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void expectSerdes() {
        expect(context.keySerde()).andStubReturn((Serde) Serdes.String());
        expect(context.valueSerde()).andStubReturn((Serde) Serdes.Long());
    }

    private void init() {
        replay(inner, context);
        metered.init((StateStoreContext) context, metered);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final KeyValueStore<Bytes, byte[]> inner = mock(InMemoryKeyValueStore.class);
        final MeteredTimestampedKeyValueStore<String, String> outer = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((ProcessorContext) context, outer);
        expectLastCall();
        replay(inner, context);
        outer.init((ProcessorContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final KeyValueStore<Bytes, byte[]> inner = mock(InMemoryKeyValueStore.class);
        final MeteredTimestampedKeyValueStore<String, String> outer = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((StateStoreContext) context, outer);
        expectLastCall();
        replay(inner, context);
        outer.init((StateStoreContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME, taskId.topologyName());
        expect(context.changelogFor(STORE_NAME)).andReturn(null);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = niceMock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<ValueAndTimestamp<String>> valueSerde = niceMock(Serde.class);
        final Deserializer<ValueAndTimestamp<String>> valueDeserializer = mock(Deserializer.class);
        final Serializer<ValueAndTimestamp<String>> valueSerializer = mock(Serializer.class);
        expect(keySerde.serializer()).andStubReturn(keySerializer);
        expect(keySerializer.serialize(topic, KEY)).andStubReturn(KEY.getBytes());
        expect(valueSerde.deserializer()).andStubReturn(valueDeserializer);
        expect(valueDeserializer.deserialize(topic, VALUE_AND_TIMESTAMP_BYTES)).andStubReturn(VALUE_AND_TIMESTAMP);
        expect(valueSerde.serializer()).andStubReturn(valueSerializer);
        expect(valueSerializer.serialize(topic, VALUE_AND_TIMESTAMP)).andStubReturn(VALUE_AND_TIMESTAMP_BYTES);
        expect(inner.get(KEY_BYTES)).andStubReturn(VALUE_AND_TIMESTAMP_BYTES);
        replay(inner, context, keySerializer, keySerde, valueDeserializer, valueSerializer, valueSerde);
        metered = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        metered.init((StateStoreContext) context, metered);

        metered.get(KEY);
        metered.put(KEY, VALUE_AND_TIMESTAMP);

        verify(keySerializer, valueDeserializer, valueSerializer);
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
            taskId.toString(),
            STORE_TYPE,
            STORE_NAME
        )));
    }
    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(eq(KEY_BYTES), aryEq(VALUE_AND_TIMESTAMP_BYTES));
        expectLastCall();
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetWithBinary() {
        expect(inner.get(KEY_BYTES)).andReturn(VALUE_AND_TIMESTAMP_BYTES);

        inner.put(eq(KEY_BYTES), aryEq(VALUE_AND_TIMESTAMP_BYTES));
        expectLastCall();
        init();

        final RawAndDeserializedValue<String> valueWithBinary = metered.getWithBinary(KEY);
        assertEquals(valueWithBinary.value, VALUE_AND_TIMESTAMP);
        assertEquals(valueWithBinary.serializedValue, VALUE_AND_TIMESTAMP_BYTES);
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldNotPutIfSameValuesAndGreaterTimestamp() {
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);
        final ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String());
        final byte[] encodedOldValue = stringSerde.serializer().serialize("TOPIC", VALUE_AND_TIMESTAMP);

        final ValueAndTimestamp<String> newValueAndTimestamp = ValueAndTimestamp.make("value", 98L);
        assertFalse(metered.putIfDifferentValues(KEY, newValueAndTimestamp, encodedOldValue));
        verify(inner);
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldPutIfOutOfOrder() {
        inner.put(eq(KEY_BYTES), aryEq(VALUE_AND_TIMESTAMP_BYTES));
        expectLastCall();
        init();

        metered.put(KEY, VALUE_AND_TIMESTAMP);

        final ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String());
        final byte[] encodedOldValue = stringSerde.serializer().serialize("TOPIC", VALUE_AND_TIMESTAMP);

        final ValueAndTimestamp<String> outOfOrderValueAndTimestamp = ValueAndTimestamp.make("value", 95L);
        assertTrue(metered.putIfDifferentValues(KEY, outOfOrderValueAndTimestamp, encodedOldValue));
        verify(inner);
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        expect(inner.get(KEY_BYTES)).andReturn(VALUE_AND_TIMESTAMP_BYTES);
        init();

        assertThat(metered.get(KEY), equalTo(VALUE_AND_TIMESTAMP));

        final KafkaMetric metric = metric("get-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        expect(inner.putIfAbsent(eq(KEY_BYTES), aryEq(VALUE_AND_TIMESTAMP_BYTES))).andReturn(null);
        init();

        metered.putIfAbsent(KEY, VALUE_AND_TIMESTAMP);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, STORE_LEVEL_GROUP, "", tags));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(anyObject(List.class));
        expectLastCall();
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(KEY, VALUE_AND_TIMESTAMP)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        expect(inner.delete(KEY_BYTES)).andReturn(VALUE_AND_TIMESTAMP_BYTES);
        init();

        metered.delete(KEY);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        expect(inner.range(KEY_BYTES, KEY_BYTES)).andReturn(
            new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.range(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE_AND_TIMESTAMP));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("range-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        expect(inner.all())
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(VALUE_AND_TIMESTAMP));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric(new MetricName("all-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldFlushInnerWhenFlushTimeRecords() {
        inner.flush();
        expectLastCall().once();
        init();

        metered.flush();

        final KafkaMetric metric = metric("flush-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private interface CachedKeyValueStore extends KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore.class);

        expect(cachedKeyValueStore.setFlushListener(anyObject(CacheFlushListener.class), eq(false))).andReturn(true);
        replay(cachedKeyValueStore);

        metered = new MeteredTimestampedKeyValueStore<>(
            cachedKeyValueStore,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String()));
        assertTrue(metered.setFlushListener(null, false));

        verify(cachedKeyValueStore);
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(metered.setFlushListener(null, false));
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        final MeteredTimestampedKeyValueStore<String, Long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            null,
            null
        );
        replay(inner, context);
        store.init((StateStoreContext) context, inner);

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
    @SuppressWarnings("unchecked")
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        expect(context.keySerde()).andStubReturn((Serde) Serdes.String());
        expect(context.valueSerde()).andStubReturn((Serde) Serdes.Long());
        final MeteredTimestampedKeyValueStore<String, Long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        replay(inner, context);
        store.init((StateStoreContext) context, inner);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }
}
