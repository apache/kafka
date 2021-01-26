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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.time.Instant.ofEpochMilli;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MeteredWindowStoreTest {

    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP_FROM_0100_TO_24 = "stream-" + STORE_TYPE + "-state-metrics";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String THREAD_ID_TAG_KEY_FROM_0100_TO_24 = "client-id";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String STORE_NAME = "mocked-store";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final int WINDOW_SIZE_MS = 10;
    private static final long TIMESTAMP = 42L;

    private final String threadId = Thread.currentThread().getName();
    private InternalMockProcessorContext context;
    private final WindowStore<Bytes, byte[]> innerStoreMock = createNiceMock(WindowStore.class);
    private MeteredWindowStore<String, String> store = new MeteredWindowStore<>(
        innerStoreMock,
        WINDOW_SIZE_MS, // any size
        STORE_TYPE,
        new MockTime(),
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private String storeLevelGroup;
    private String threadIdTagKey;
    private Map<String, String> tags;

    {
        expect(innerStoreMock.name()).andReturn(STORE_NAME).anyTimes();
    }

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
    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion, new MockTime());
        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
        );
        storeLevelGroup =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? STORE_LEVEL_GROUP_FROM_0100_TO_24 : STORE_LEVEL_GROUP;
        threadIdTagKey =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? THREAD_ID_TAG_KEY_FROM_0100_TO_24 : THREAD_ID_TAG_KEY;
        tags = mkMap(
            mkEntry(threadIdTagKey, threadId),
            mkEntry("task-id", context.taskId().toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final WindowStore<Bytes, byte[]> inner = mock(WindowStore.class);
        final MeteredWindowStore<String, String> outer = new MeteredWindowStore<>(
            inner,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((ProcessorContext) context, outer);
        expectLastCall();
        replay(inner);
        outer.init((ProcessorContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final WindowStore<Bytes, byte[]> inner = mock(WindowStore.class);
        final MeteredWindowStore<String, String> outer = new MeteredWindowStore<>(
            inner,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((StateStoreContext) context, outer);
        expectLastCall();
        replay(inner);
        outer.init((StateStoreContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        context.addChangelogForStore(STORE_NAME, CHANGELOG_TOPIC);
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), STORE_NAME);
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
        expect(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).andStubReturn(VALUE_BYTES);
        replay(innerStoreMock, keySerializer, keySerde, valueDeserializer, valueSerializer, valueSerde);
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

        verify(keySerializer, valueDeserializer, valueSerializer);
    }

    @Test
    public void testMetrics() {
        replay(innerStoreMock);
        store.init((StateStoreContext) context, store);
        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
            storeLevelGroup,
            threadIdTagKey,
            threadId,
            context.taskId().toString(),
            STORE_TYPE,
            STORE_NAME
        )));
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
                storeLevelGroup,
                threadIdTagKey,
                threadId,
                context.taskId().toString(),
                STORE_TYPE,
                ROLLUP_VALUE
            )));
        }
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() {
        innerStoreMock.init((StateStoreContext) context, store);
        expectLastCall();
        replay(innerStoreMock);
        store.init((StateStoreContext) context, store);
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "restore-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", STORE_NAME)
            ).metricValue());
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "restore-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", ROLLUP_VALUE)
            ).metricValue());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldRecordPutLatency() {
        final byte[] bytes = "a".getBytes();
        innerStoreMock.put(eq(Bytes.wrap(bytes)), anyObject(), eq(context.timestamp()));
        expectLastCall();
        replay(innerStoreMock);

        store.init((StateStoreContext) context, store);
        store.put("a", "a");
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "put-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", STORE_NAME)
            ).metricValue());
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "put-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", ROLLUP_VALUE)
            ).metricValue());
        }
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());
        replay(innerStoreMock);

        store.init((StateStoreContext) context, store);
        store.fetch("a", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "fetch-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", STORE_NAME)
            ).metricValue());
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "fetch-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", ROLLUP_VALUE)
            ).metricValue());
        }
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchRangeLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());
        replay(innerStoreMock);

        store.init((StateStoreContext) context, store);
        store.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "fetch-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", STORE_NAME)
            ).metricValue());
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "fetch-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", ROLLUP_VALUE)
            ).metricValue());
        }
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFlushLatency() {
        innerStoreMock.flush();
        expectLastCall();
        replay(innerStoreMock);

        store.init((StateStoreContext) context, store);
        store.flush();
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "flush-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", STORE_NAME)
            ).metricValue());
            assertEquals(1.0, getMetricByNameFilterByTags(
                metrics,
                "flush-total",
                storeLevelGroup,
                singletonMap(STORE_TYPE + "-state-id", ROLLUP_VALUE)
            ).metricValue());
        }
        verify(innerStoreMock);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchReturnsNull() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        replay(innerStoreMock);

        store.init((StateStoreContext) context, store);
        assertNull(store.fetch("a", 0));
    }

    private interface CachedWindowStore extends WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedWindowStore cachedWindowStore = mock(CachedWindowStore.class);

        expect(cachedWindowStore.setFlushListener(anyObject(CacheFlushListener.class), eq(false))).andReturn(true);
        replay(cachedWindowStore);

        final MeteredWindowStore<String, String> metered = new MeteredWindowStore<>(
            cachedWindowStore,
            10L, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        assertTrue(metered.setFlushListener(null, false));

        verify(cachedWindowStore);
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(store.setFlushListener(null, false));
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        expectLastCall();
        replay(innerStoreMock);
        store.init((StateStoreContext) context, store);

        store.close();
        verify(innerStoreMock);
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        innerStoreMock.close();
        expectLastCall();
        replay(innerStoreMock);
        store.init((StateStoreContext) context, store);

        assertThat(storeMetrics(), not(empty()));
        store.close();
        assertThat(storeMetrics(), empty());
        verify(innerStoreMock);
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        innerStoreMock.close();
        expectLastCall().andThrow(new RuntimeException("Oops!"));
        replay(innerStoreMock);
        store.init((StateStoreContext) context, store);

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
        verify(innerStoreMock);
    }

    private List<MetricName> storeMetrics() {
        return metrics.metrics()
            .keySet()
            .stream()
            .filter(name -> name.group().equals(storeLevelGroup) && name.tags().equals(tags))
            .collect(Collectors.toList());
    }
}
