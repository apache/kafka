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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.state.internals.StoreMetrics.ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.DELETE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.FLUSH;
import static org.apache.kafka.streams.state.internals.StoreMetrics.GET;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_IF_ABSENT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RANGE;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class MeteredTimestampedKeyValueStoreTest {

    private final TaskId taskId = new TaskId(0, 0);
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContext context;

    private MeteredTimestampedKeyValueStore<String, String> metered;
    private final String key = "key";
    private final Bytes keyBytes = Bytes.wrap(key.getBytes());
    private final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make("value", 97L);
    // timestamp is 97 what is ASCII of 'a'
    private final byte[] valueAndTimestampBytes = "\0\0\0\0\0\0\0avalue".getBytes();
    private final KeyValue<Bytes, byte[]> byteKeyValueTimestampPair = KeyValue.pair(keyBytes, valueAndTimestampBytes);
    private final Metrics metrics = new Metrics();
    private final String scope = "scope";

    @Before
    public void before() {
        metered = new MeteredTimestampedKeyValueStore<>(
            inner,
            scope,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        expect(context.metrics()).andReturn(new StreamsMetricsImpl(metrics));
        expect(context.taskId()).andReturn(taskId);
        expect(inner.name()).andReturn("metered").anyTimes();
    }

    private void init() {
        replay(inner, context);
        metered.init(context, metered);
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(eq(keyBytes), aryEq(valueAndTimestampBytes));
        expectLastCall();
        init();

        metered.put(key, valueAndTimestamp);

        final Metric metric = getMetricByName(metrics.metrics(), PUT + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        expect(inner.get(keyBytes)).andReturn(valueAndTimestampBytes);
        init();

        assertThat(metered.get(key), equalTo(valueAndTimestamp));

        final Metric metric = getMetricByName(metrics.metrics(), GET + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        expect(inner.putIfAbsent(eq(keyBytes), aryEq(valueAndTimestampBytes))).andReturn(null);
        init();

        metered.putIfAbsent(key, valueAndTimestamp);

        final Metric metric = getMetricByName(metrics.metrics(), PUT_IF_ABSENT + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(anyObject(List.class));
        expectLastCall();
        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(key, valueAndTimestamp)));

        final Metric metric = getMetricByName(metrics.metrics(), PUT_ALL + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        expect(inner.delete(keyBytes)).andReturn(valueAndTimestampBytes);
        init();

        metered.delete(key);

        final Metric metric = getMetricByName(metrics.metrics(), DELETE + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        expect(inner.range(keyBytes, keyBytes)).andReturn(
            new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.range(key, key);
        assertThat(iterator.next().value, equalTo(valueAndTimestamp));
        assertFalse(iterator.hasNext());
        iterator.close();

        final Metric metric = getMetricByName(metrics.metrics(), RANGE + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        expect(inner.all())
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        final KeyValueIterator<String, ValueAndTimestamp<String>> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(valueAndTimestamp));
        assertFalse(iterator.hasNext());
        iterator.close();

        final Metric metric = getMetricByName(metrics.metrics(), ALL + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

        assertTrue((Double) metric.metricValue() > 0);
        verify(inner);
    }

    @Test
    public void shouldFlushInnerWhenFlushTimeRecords() {
        inner.flush();
        expectLastCall().once();
        init();

        metered.flush();

        final Metric metric = getMetricByName(metrics.metrics(), FLUSH + RATE_SUFFIX, StreamsMetricsImpl.groupNameFromScope(scope));

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
            scope,
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
}
