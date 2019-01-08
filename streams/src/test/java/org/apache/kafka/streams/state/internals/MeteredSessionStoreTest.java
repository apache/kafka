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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class MeteredSessionStoreTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final Map<String, String> tags = mkMap(
        mkEntry("client-id", "test"),
        mkEntry("task-id", taskId.toString()),
        mkEntry("scope-id", "metered")
    );
    private final Metrics metrics = new Metrics();
    private MeteredSessionStore<String, String> metered;
    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContext context;

    private final String key = "a";
    private final byte[] keyBytes = key.getBytes();
    private final Windowed<Bytes> windowedKeyBytes = new Windowed<>(Bytes.wrap(keyBytes), new SessionWindow(0, 0));


    @Before
    public void before() {
        metered = new MeteredSessionStore<>(inner,
                                            "scope",
                                            Serdes.String(),
                                            Serdes.String(),
                                            new MockTime());
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
    public void testMetrics() {
        init();
        final JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "metered")));
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "all")));
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(EasyMock.eq(windowedKeyBytes), EasyMock.aryEq(keyBytes));
        EasyMock.expectLastCall();
        init();

        metered.put(new Windowed<>(key, new SessionWindow(0, 0)), key);

        final KafkaMetric metric = metric("put-rate");
        assertTrue(((Double) metric.metricValue()) > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldFindSessionsFromStoreAndRecordFetchMetric() {
        EasyMock.expect(inner.findSessions(Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(windowedKeyBytes, keyBytes)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = metered.findSessions(key, 0, 0);
        assertThat(iterator.next().value, equalTo(key));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        EasyMock.expect(inner.findSessions(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(windowedKeyBytes, keyBytes)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = metered.findSessions(key, key, 0, 0);
        assertThat(iterator.next().value, equalTo(key));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldRemoveFromStoreAndRecordRemoveMetric() {
        inner.remove(windowedKeyBytes);
        EasyMock.expectLastCall();

        init();

        metered.remove(new Windowed<>(key, new SessionWindow(0, 0)));

        final KafkaMetric metric = metric("remove-rate");
        assertTrue((Double) metric.metricValue() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldFetchForKeyAndRecordFetchMetric() {
        EasyMock.expect(inner.findSessions(Bytes.wrap(keyBytes), 0, Long.MAX_VALUE))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(windowedKeyBytes, keyBytes)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = metered.fetch(key);
        assertThat(iterator.next().value, equalTo(key));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldFetchRangeFromStoreAndRecordFetchMetric() {
        EasyMock.expect(inner.findSessions(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes), 0, Long.MAX_VALUE))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(windowedKeyBytes, keyBytes)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = metered.fetch(key, key);
        assertThat(iterator.next().value, equalTo(key));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldRecordRestoreTimeOnInit() {
        init();
        final KafkaMetric metric = metric("restore-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        metered.put(null, "a");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        metered.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        metered.fetch(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        metered.fetch(null, "to");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchRangeIfToIsNull() {
        metered.fetch("from", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        metered.findSessions(null, 0, 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        metered.findSessions(null, "a", 0, 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        metered.findSessions("a", null, 0, 0);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

}