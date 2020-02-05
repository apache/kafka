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
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
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
import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MeteredWindowStoreTest {

    private static final String STORE_TYPE = "scope";
    private static final String STORE_LEVEL_GROUP_FROM_0100_TO_24 = "stream-" + STORE_TYPE + "-state-metrics";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String THREAD_ID_TAG_KEY_FROM_0100_TO_24 = "client-id";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String STORE_NAME = "mocked-store";

    private final String threadId = Thread.currentThread().getName();
    private InternalMockProcessorContext context;
    @SuppressWarnings("unchecked")
    private final WindowStore<Bytes, byte[]> innerStoreMock = createNiceMock(WindowStore.class);
    private final MeteredWindowStore<String, String> store = new MeteredWindowStore<>(
        innerStoreMock,
        10L, // any size
        STORE_TYPE,
        new MockTime(),
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private String storeLevelGroup;
    private String threadIdTagKey;

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
            new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion);

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
    }

    @Test
    public void testMetrics() {
        replay(innerStoreMock);
        store.init(context, store);
        final JmxReporter reporter = new JmxReporter("kafka.streams");
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
        innerStoreMock.init(context, store);
        expectLastCall();
        replay(innerStoreMock);
        store.init(context, store);
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

        store.init(context, store);
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

        store.init(context, store);
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

        store.init(context, store);
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

        store.init(context, store);
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
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.close();
        verify(innerStoreMock);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchReturnsNull() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    private interface CachedWindowStore extends WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

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
}
