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
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MeteredKeyValueStoreTest {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_NAME = "store-name";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP_FROM_0100_TO_24 = "stream-" + STORE_TYPE + "-state-metrics";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String THREAD_ID_TAG_KEY_FROM_0100_TO_24 = "client-id";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final KeyValue<Bytes, byte[]> BYTE_KEY_VALUE_PAIR = KeyValue.pair(KEY_BYTES, VALUE_BYTES);

    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0);

    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private MeteredKeyValueStore<String, String> metered;
    private final Metrics metrics = new Metrics();
    private String storeLevelGroup;
    private String threadIdTagKey;
    private Map<String, String> tags;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {StreamsConfig.METRICS_LATEST},
            {StreamsConfig.METRICS_0100_TO_24}
        });
    }

    @Parameter
    public String builtInMetricsVersion;

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
        expect(context.applicationId()).andStubReturn(APPLICATION_ID);
        expect(context.metrics()).andStubReturn(
            new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion, mockTime)
        );
        expect(context.taskId()).andStubReturn(taskId);
        expect(context.changelogFor(STORE_NAME)).andStubReturn(CHANGELOG_TOPIC);
        expect(inner.name()).andStubReturn(STORE_NAME);
        storeLevelGroup =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? STORE_LEVEL_GROUP_FROM_0100_TO_24 : STORE_LEVEL_GROUP;
        threadIdTagKey =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? THREAD_ID_TAG_KEY_FROM_0100_TO_24 : THREAD_ID_TAG_KEY;
        tags = mkMap(
            mkEntry(threadIdTagKey, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    private void init() {
        replay(inner, context);
        metered.init((StateStoreContext) context, metered);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final KeyValueStore<Bytes, byte[]> inner = mock(KeyValueStore.class);
        final MeteredKeyValueStore<String, String> outer = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
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
        final KeyValueStore<Bytes, byte[]> inner = mock(KeyValueStore.class);
        final MeteredKeyValueStore<String, String> outer = new MeteredKeyValueStore<>(
            inner,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
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
        final String defaultChangelogTopicName = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME);
        expect(context.changelogFor(STORE_NAME)).andReturn(null);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = niceMock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<String> valueSerde = niceMock(Serde.class);
        final Deserializer<String> valueDeserializer = mock(Deserializer.class);
        final Serializer<String> valueSerializer = mock(Serializer.class);
        expect(keySerde.serializer()).andStubReturn(keySerializer);
        expect(keySerializer.serialize(topic, KEY)).andStubReturn(KEY.getBytes());
        expect(valueSerde.deserializer()).andStubReturn(valueDeserializer);
        expect(valueDeserializer.deserialize(topic, VALUE_BYTES)).andStubReturn(VALUE);
        expect(valueSerde.serializer()).andStubReturn(valueSerializer);
        expect(valueSerializer.serialize(topic, VALUE)).andStubReturn(VALUE_BYTES);
        expect(inner.get(KEY_BYTES)).andStubReturn(VALUE_BYTES);
        replay(inner, context, keySerializer, keySerde, valueDeserializer, valueSerializer, valueSerde);
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
            storeLevelGroup,
            threadIdTagKey,
            threadId,
            taskId.toString(),
            STORE_TYPE,
            STORE_NAME
        )));
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
                storeLevelGroup,
                threadIdTagKey,
                threadId,
                taskId.toString(),
                STORE_TYPE,
                ROLLUP_VALUE
            )));
        }
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(eq(KEY_BYTES), aryEq(VALUE_BYTES));
        expectLastCall();
        init();

        metered.put(KEY, VALUE);

        final KafkaMetric metric = metric("put-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        expect(inner.get(KEY_BYTES)).andReturn(VALUE_BYTES);
        init();

        assertThat(metered.get(KEY), equalTo(VALUE));

        final KafkaMetric metric = metric("get-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        expect(inner.putIfAbsent(eq(KEY_BYTES), aryEq(VALUE_BYTES))).andReturn(null);
        init();

        metered.putIfAbsent(KEY, VALUE);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(anyObject(List.class));
        expectLastCall();
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(KEY, VALUE)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        expect(inner.delete(KEY_BYTES)).andReturn(VALUE_BYTES);
        init();

        metered.delete(KEY);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        expect(inner.range(KEY_BYTES, KEY_BYTES))
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.range(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("range-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        expect(inner.all()).andReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric(new MetricName("all-rate", storeLevelGroup, "", tags));
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

        metered = new MeteredKeyValueStore<>(
            cachedKeyValueStore,
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
        assertTrue(metered.setFlushListener(null, false));

        verify(cachedKeyValueStore);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfGetReturnsNull() {
        expect(inner.get(Bytes.wrap("a".getBytes()))).andReturn(null);

        init();
        assertNull(metered.get("a"));
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(metered.setFlushListener(null, false));
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        inner.close();
        expectLastCall();
        init(); // replays "inner"

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        metered.close();
        assertThat(storeMetrics(), empty());
        verify(inner);
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        inner.close();
        expectLastCall().andThrow(new RuntimeException("Oops!"));
        init(); // replays "inner"

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, metered::close);
        assertThat(storeMetrics(), empty());
        verify(inner);
    }

    @Test
    public void shouldGetRecordsWithPrefixKey() {
        final StringSerializer stringSerializer = new StringSerializer();
        expect(inner.prefixScan(KEY, stringSerializer))
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(BYTE_KEY_VALUE_PAIR).iterator()));
        init();

        final KeyValueIterator<String, String> iterator = metered.prefixScan(KEY, stringSerializer);
        assertThat(iterator.next().value, equalTo(VALUE));
        iterator.close();

        final KafkaMetric metric = metrics.metric(new MetricName("prefix-scan-rate", STORE_LEVEL_GROUP, "", tags));
        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
    }

    private KafkaMetric metric(final String name) {
        return metrics.metric(new MetricName(name, storeLevelGroup, "", tags));
    }

    private List<MetricName> storeMetrics() {
        return metrics.metrics()
                      .keySet()
                      .stream()
                      .filter(name -> name.group().equals(storeLevelGroup) && name.tags().equals(tags))
                      .collect(Collectors.toList());
    }
}
