/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.SegmentedBytesStoreStub;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class MeteredSegmentedBytesStoreTest {
    private final SegmentedBytesStoreStub bytesStore = new SegmentedBytesStoreStub();
    private final MeteredSegmentedBytesStore store = new MeteredSegmentedBytesStore(bytesStore, "scope", new MockTime());
    private final Set<String> latencyRecorded = new HashSet<>();
    private final Set<String> throughputRecorded = new HashSet<>();

    @SuppressWarnings("unchecked")
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

        final MockProcessorContext context = new MockProcessorContext(TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      new NoOpRecordCollector(),
                                                                      new ThreadCache("testCache", 0, streamsMetrics)) {
            @Override
            public StreamsMetrics metrics() {
                return streamsMetrics;
            }
        };
        store.init(context, store);
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() throws Exception {
        assertTrue(latencyRecorded.contains("restore"));
        assertTrue(bytesStore.initialized);
    }

    @Test
    public void shouldRecordPutLatency() throws Exception {
        store.put(Bytes.wrap(new byte[0]), new byte[0]);
        assertTrue(latencyRecorded.contains("put"));
        assertTrue(bytesStore.putCalled);
    }

    @Test
    public void shouldRecordFetchLatency() throws Exception {
        store.fetch(Bytes.wrap(new byte[0]), 1, 1).close(); // recorded on close;
        assertTrue(latencyRecorded.contains("fetch"));
        assertTrue(bytesStore.fetchCalled);
    }

    @Test
    public void shouldRecordRemoveLatency() throws Exception {
        store.remove(null);
        assertTrue(latencyRecorded.contains("remove"));
        assertTrue(bytesStore.removeCalled);
    }

    @Test
    public void shouldRecordFlushLatency() throws Exception {
        store.flush();
        assertTrue(latencyRecorded.contains("flush"));
        assertTrue(bytesStore.flushed);
    }

    @Test
    public void shouldRecordGetLatency() throws Exception {
        store.get(null);
        assertTrue(latencyRecorded.contains("get"));
        assertTrue(bytesStore.getCalled);
    }

    @Test
    public void shouldCloseUnderlyingStore() throws Exception {
        store.close();
        assertTrue(bytesStore.closed);
    }


}