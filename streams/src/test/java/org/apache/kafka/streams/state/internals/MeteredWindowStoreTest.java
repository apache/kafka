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
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MeteredWindowStoreTest {
    private InternalMockProcessorContext context;
    @SuppressWarnings("unchecked")
    private final WindowStore<Bytes, byte[]> innerStoreMock = EasyMock.createNiceMock(WindowStore.class);
    private final MeteredWindowStore<String, String> store = new MeteredWindowStore<>(
        innerStoreMock,
        "scope",
        new MockTime(),
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );

    {
        EasyMock.expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
    }

    @Before
    public void setUp() {
        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test");

        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.minimalStreamsConfig()),
            new RecordCollector.Supplier() {
                @Override
                public RecordCollector recordCollector() {
                    return new NoOpRecordCollector();
                }
            },
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
        );
    }

    @After
    public void after() {
        context.close();
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() {
        innerStoreMock.init(context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);
        store.init(context, store);
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
    }

    @Test
    public void shouldRecordPutLatency() {
        final byte[] bytes = "a".getBytes();
        innerStoreMock.put(EasyMock.eq(Bytes.wrap(bytes)), EasyMock.<byte[]>anyObject(), EasyMock.eq(context.timestamp()));
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.put("a", "a");
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchLatency() {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", 1, 1).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchRangeLatency() {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", "b", 1, 1).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        EasyMock.verify(innerStoreMock);
    }


    @Test
    public void shouldRecordFlushLatency() {
        innerStoreMock.flush();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.flush();
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        EasyMock.verify(innerStoreMock);
    }


    @Test
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.close();
        EasyMock.verify(innerStoreMock);
    }


    @Test
    public void shouldNotExceptionIfFetchReturnsNull() {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

}