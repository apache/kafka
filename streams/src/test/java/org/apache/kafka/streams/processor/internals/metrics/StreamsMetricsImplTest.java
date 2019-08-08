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
package org.apache.kafka.streams.processor.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.utils.MockTime;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxLatencyToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamsMetricsImplTest extends EasyMockSupport {

    private final static String SENSOR_PREFIX_DELIMITER = ".";
    private final static String SENSOR_NAME_DELIMITER = ".s.";
    private final static String INTERNAL_PREFIX = "internal";

    private final Metrics metrics = new Metrics();
    private final Sensor sensor = metrics.sensor("dummy");
    private final String metricNamePrefix = "metric";
    private final String group = "group";
    private final Map<String, String> tags = mkMap(mkEntry("tag", "value"));
    private final String description1 = "description number one";
    private final String description2 = "description number two";
    private final MockTime time = new MockTime(0);

    @Test
    public void shouldGetThreadLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final String threadName = "thread1";
        final String sensorName = "sensor1";
        final String expectedFullSensorName =
            INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + sensorName;
        final RecordingLevel recordingLevel = RecordingLevel.DEBUG;
        final Sensor[] parents = {};
        EasyMock.expect(metrics.sensor(expectedFullSensorName, recordingLevel, parents)).andReturn(null);

        replayAll();

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadName);
        final Sensor sensor = streamsMetrics.threadLevelSensor(sensorName, recordingLevel);

        verifyAll();

        assertNull(sensor);
    }

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

        assertEquals(Collections.emptyMap(), streamsMetrics.parentSensors());
    }

    @Test
    public void testMutiLevelSensorRemoval() {
        final Metrics registry = new Metrics();
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, "");
        for (final MetricName defaultMetric : registry.metrics().keySet()) {
            registry.removeMetric(defaultMetric);
        }

        final String taskName = "taskName";
        final String operation = "operation";
        final Map<String, String> taskTags = mkMap(mkEntry("tkey", "value"));

        final String processorNodeName = "processorNodeName";
        final Map<String, String> nodeTags = mkMap(mkEntry("nkey", "value"));

        final Sensor parent1 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        final int numberOfTaskMetrics = registry.metrics().size();

        final Sensor sensor1 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent1);
        addAvgAndMaxLatencyToSensor(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor parent2 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor sensor2 = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent2);
        addAvgAndMaxLatencyToSensor(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        metrics.removeAllTaskLevelSensors(taskName);

        assertThat(registry.metrics().size(), equalTo(0));
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
            assertEquals(i, Math.round(totalMetric.measurable().measure(config, time.milliseconds())));
            sensor.record(latency, time.milliseconds());
        }
    }

    @Test
    public void shouldGetStoreLevelTagMap() {
        final String threadName = "test-thread";
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadName);
        final String taskName = "test-task";
        final String storeType = "remote-window";
        final String storeName = "window-keeper";

        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskName, storeType, storeName);

        assertThat(tagMap.size(), equalTo(3));
        assertThat(tagMap.get(StreamsMetricsImpl.THREAD_ID_TAG), equalTo(threadName));
        assertThat(tagMap.get(StreamsMetricsImpl.TASK_ID_TAG), equalTo(taskName));
        assertThat(tagMap.get(storeType + "-" + StreamsMetricsImpl.STORE_ID_TAG), equalTo(storeName));
    }

    @Test
    public void shouldAddAmountRateAndSum() {
        StreamsMetricsImpl
            .addRateOfSumAndSumMetricsToSensor(sensor, group, tags, metricNamePrefix, description1, description2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 72.0;
        final long defaultWindowSizeInSeconds = Duration.ofMillis(new MetricConfig().timeWindowMs()).getSeconds();
        final double expectedRateMetricValue = (valueToRecord1 + valueToRecord2) / defaultWindowSizeInSeconds;
        verifyMetric(metricNamePrefix + "-rate", description1, valueToRecord1, valueToRecord2, expectedRateMetricValue);
        final double expectedSumMetricValue = 2 * valueToRecord1 + 2 * valueToRecord2; // values are recorded once for each metric verification
        verifyMetric(metricNamePrefix + "-total", description2, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddSum() {
        StreamsMetricsImpl.addSumMetricToSensor(sensor, group, tags, metricNamePrefix, description1);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedSumMetricValue = valueToRecord1 + valueToRecord2;
        verifyMetric(metricNamePrefix + "-total", description1, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(1 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddAmountRate() {
        StreamsMetricsImpl.addRateOfSumMetricToSensor(sensor, group, tags, metricNamePrefix, description1);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 72.0;
        final long defaultWindowSizeInSeconds = Duration.ofMillis(new MetricConfig().timeWindowMs()).getSeconds();
        final double expectedRateMetricValue = (valueToRecord1 + valueToRecord2) / defaultWindowSizeInSeconds;
        verifyMetric(metricNamePrefix + "-rate", description1, valueToRecord1, valueToRecord2, expectedRateMetricValue);
        assertThat(metrics.metrics().size(), equalTo(1 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddValue() {
        StreamsMetricsImpl.addValueMetricToSensor(sensor, group, tags, metricNamePrefix, description1);

        final KafkaMetric ratioMetric = metrics.metric(new MetricName(metricNamePrefix, group, description1, tags));
        assertThat(ratioMetric, is(notNullValue()));
        final MetricConfig metricConfig = new MetricConfig();
        final double value1 = 42.0;
        sensor.record(value1);
        assertThat(ratioMetric.measurable().measure(metricConfig, time.milliseconds()), equalTo(42.0));
        final double value2 = 18.0;
        sensor.record(value2);
        assertThat(ratioMetric.measurable().measure(metricConfig, time.milliseconds()), equalTo(18.0));
        assertThat(metrics.metrics().size(), equalTo(1 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddAvgAndTotalMetricsToSensor() {
        StreamsMetricsImpl
            .addAvgAndSumMetricsToSensor(sensor, group, tags, metricNamePrefix, description1, description2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedAvgMetricValue = (valueToRecord1 + valueToRecord2) / 2;
        verifyMetric(metricNamePrefix + "-avg", description1, valueToRecord1, valueToRecord2, expectedAvgMetricValue);
        final double expectedSumMetricValue = 2 * valueToRecord1 + 2 * valueToRecord2; // values are recorded once for each metric verification
        verifyMetric(metricNamePrefix + "-total", description2, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    private void verifyMetric(final String name,
                              final String description,
                              final double valueToRecord1,
                              final double valueToRecord2,
                              final double expectedMetricValue) {
        final KafkaMetric metric = metrics
            .metric(new MetricName(name, group, description, tags));
        assertThat(metric, is(notNullValue()));
        assertThat(metric.metricName().description(), equalTo(description));
        sensor.record(valueToRecord1, time.milliseconds());
        sensor.record(valueToRecord2, time.milliseconds());
        assertThat(
            metric.measurable().measure(new MetricConfig(), time.milliseconds()),
            equalTo(expectedMetricValue)
        );
    }
}
