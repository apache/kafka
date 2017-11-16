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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class MeteredWindowStoreTest {
    private MockProcessorContext context;
    @SuppressWarnings("unchecked")
    private final WindowStore<Bytes, byte[]> innerStoreMock = EasyMock.createNiceMock(WindowStore.class);
    private final MeteredWindowStore<String, String> store = new MeteredWindowStore<>(innerStoreMock, "scope", new MockTime(), Serdes.String(), Serdes.String());
    private final Set<String> latencyRecorded = new HashSet<>();
    private final Set<String> throughputRecorded = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        final Metrics metrics = new Metrics();
        final StreamsMetrics streamsMetrics = new StreamsMetrics() {

            @Override
            public Map<MetricName, ? extends Metric> metrics() {
                return Collections.unmodifiableMap(metrics.metrics());
            }

            @Override
            public Sensor addLatencyAndThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordingLevel recordLevel, String... tags) {
                return metrics.sensor(operationName);
            }

            @Override
            public void recordLatency(final Sensor sensor, final long startNs, final long endNs) {
                latencyRecorded.add(sensor.name());
            }

            @Override
            public Sensor addThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordingLevel recordLevel, String... tags) {
                return metrics.sensor(operationName);
            }

            @Override
            public void recordThroughput(Sensor sensor, long value) {
                throughputRecorded.add(sensor.name());
            }

            @Override
            public void removeSensor(Sensor sensor) {
                metrics.removeSensor(sensor.name());
            }

            @Override
            public Sensor addSensor(String name, Sensor.RecordingLevel recordLevel) {
                return metrics.sensor(name);
            }

            @Override
            public Sensor addSensor(String name, Sensor.RecordingLevel recordLevel, Sensor... parents) {
                return metrics.sensor(name);
            }

        };

        context = new MockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)) {

            @Override
            public StreamsMetrics metrics() {
                return streamsMetrics;
            }
        };
        EasyMock.expect(innerStoreMock.name()).andReturn("store").anyTimes();
    }

    @After
    public void after() {
        context.close();
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() throws Exception {
        innerStoreMock.init(context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);
        store.init(context, store);
        assertTrue(latencyRecorded.contains("restore"));
    }

    @Test
    public void shouldRecordPutLatency() throws Exception {
        final byte[] bytes = "a".getBytes();
        innerStoreMock.put(EasyMock.eq(Bytes.wrap(bytes)), EasyMock.<byte[]>anyObject(), EasyMock.eq(context.timestamp()));
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.put("a", "a");
        assertTrue(latencyRecorded.contains("put"));
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchLatency() throws Exception {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", 1, 1).close(); // recorded on close;
        assertTrue(latencyRecorded.contains("fetch"));
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchRangeLatency() throws Exception {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", "b", 1, 1).close(); // recorded on close;
        assertTrue(latencyRecorded.contains("fetch"));
        EasyMock.verify(innerStoreMock);
    }


    @Test
    public void shouldRecordFlushLatency() throws Exception {
        innerStoreMock.flush();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.flush();
        assertTrue(latencyRecorded.contains("flush"));
        EasyMock.verify(innerStoreMock);
    }


    @Test
    public void shouldCloseUnderlyingStore() throws Exception {
        innerStoreMock.close();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.close();
        EasyMock.verify(innerStoreMock);
    }


}