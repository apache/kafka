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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class MeteredKeyValueBytesStoreTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final Map<String, String> tags = new HashMap<>();
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContext context;

    private MeteredKeyValueBytesStore<String, String> metered;
    private final String key = "key";
    private final Bytes keyBytes = Bytes.wrap(key.getBytes());
    private final String value = "value";
    private final byte[] valueBytes = value.getBytes();
    private final Metrics metrics = new Metrics();

    @Before
    public void before() {
        metered = new MeteredKeyValueBytesStore<>(inner,
                                                  "scope",
                                                  new MockTime(),
                                                  Serdes.String(),
                                                  Serdes.String());
        tags.put("task-id", taskId.toString());
        tags.put("scope-id", "metered");
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        EasyMock.expect(context.metrics()).andReturn(new MockStreamsMetrics(metrics));
        EasyMock.expect(context.taskId()).andReturn(taskId);
        EasyMock.expect(inner.name()).andReturn("metered").anyTimes();
    }

    private void init() {
        EasyMock.replay(inner, context);
        metered.init(context, metered);
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(EasyMock.eq(keyBytes), EasyMock.aryEq(valueBytes));
        EasyMock.expectLastCall();

        init();

        metered.put(key, value);

        final KafkaMetric metric = metric("put-rate");

        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }


    @Test
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        EasyMock.expect(inner.get(keyBytes)).andReturn(valueBytes);
        init();

        assertThat(metered.get(key), equalTo(value));

        final KafkaMetric metric = metric("get-rate");
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        EasyMock.expect(inner.putIfAbsent(EasyMock.eq(keyBytes), EasyMock.aryEq(valueBytes)))
                .andReturn(null);
        init();

        metered.putIfAbsent(key, value);

        final KafkaMetric metric = metric("put-if-absent-rate");
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall();

        init();

        metered.putAll(Collections.singletonList(KeyValue.pair(key, value)));

        final KafkaMetric metric = metric("put-all-rate");
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        EasyMock.expect(inner.delete(keyBytes)).andReturn(valueBytes);

        init();

        metered.delete(key);

        final KafkaMetric metric = metric("delete-rate");
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        EasyMock.expect(inner.range(keyBytes, keyBytes))
                .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(KeyValue.pair(keyBytes, valueBytes)).iterator()));

        init();

        final KeyValueIterator<String, String> iterator = metered.range(key, key);
        assertThat(iterator.next().value, equalTo(value));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("range-rate");
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        EasyMock.expect(inner.all())
                .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(KeyValue.pair(keyBytes, valueBytes)).iterator()));

        init();

        final KeyValueIterator<String, String> iterator = metered.all();
        assertThat(iterator.next().value, equalTo(value));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric(new MetricName("all-rate", "stream-scope-metrics", "", tags));
        assertTrue(metric.value() > 0);
        EasyMock.verify(inner);
    }

    private KafkaMetric metric(final MetricName metricName) {
        return this.metrics.metric(metricName);
    }


}