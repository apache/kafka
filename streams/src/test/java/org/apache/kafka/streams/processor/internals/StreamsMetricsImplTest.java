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
package org.apache.kafka.streams.processor.internals;


import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class StreamsMetricsImplTest {

    @Test(expected = NullPointerException.class)
    public void testNullMetrics() {
        new StreamsMetricsImpl(null, "");
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullSensor() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        streamsMetrics.removeSensor(null);
    }

    @Test
    public void testRemoveSensor() {
        final String sensorName = "sensor1";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");

        final Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        final Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        final Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        final Sensor sensor3 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);
    }

    @Test
    public void testLatencyMetrics() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        // 2 meters and 4 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        final int otherMetricsCount = 4;
        assertEquals(defaultMetrics + meterMetricsCount * 2 + otherMetricsCount, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }

    @Test
    public void testThroughputMetrics() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(defaultMetrics + meterMetricsCount * 2, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }

    @Test
    public void testTotalMetricDoesntDecrease() {
        final MockTime time = new MockTime(1);
        final MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS);
        final Metrics metrics = new Metrics(config, time);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "");

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "op";

        final Sensor sensor = streamsMetrics.addLatencyAndThroughputSensor(
                scope,
                entity,
                operation,
                Sensor.RecordingLevel.INFO
        );

        final double latency = 100.0;
        final MetricName totalMetricName = metrics.metricName(
                "op-total",
                "stream-scope-metrics",
                "",
                "client-id",
                "",
                "scope-id",
                "entity"
        );

        final KafkaMetric totalMetric = metrics.metric(totalMetricName);

        for (int i = 0; i < 10; i++) {
            assertEquals(i, Math.round(totalMetric.measurable().measure(config, time.absoluteMilliseconds())));
            sensor.record(latency, time.absoluteMilliseconds());
        }

    }
}
