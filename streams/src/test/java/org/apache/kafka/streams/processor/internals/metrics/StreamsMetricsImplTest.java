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
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ImmutableMetricValue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CLIENT_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxLatencyToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.resetToDefault;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalToObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

public class StreamsMetricsImplTest extends EasyMockSupport {

    private final static String SENSOR_PREFIX_DELIMITER = ".";
    private final static String SENSOR_NAME_DELIMITER = ".s.";
    private final static String INTERNAL_PREFIX = "internal";
    private final static String VERSION = StreamsMetricsImpl.METRICS_LATEST;
    private final static String CLIENT_ID = "test-client";
    private final static String THREAD_ID = "test-thread";
    private final static String TASK_ID = "test-task";
    private final static String METRIC_NAME1 = "test-metric1";
    private final static String METRIC_NAME2 = "test-metric2";

    private final Metrics metrics = new Metrics();
    private final Sensor sensor = metrics.sensor("dummy");
    private final String storeName = "store";
    private final String sensorName1 = "sensor1";
    private final String sensorName2 = "sensor2";
    private final String metricNamePrefix = "metric";
    private final String group = "group";
    private final Map<String, String> tags = mkMap(mkEntry("tag", "value"));
    private final String description1 = "description number one";
    private final String description2 = "description number two";
    private final String description3 = "description number three";
    private final Map<String, String> clientLevelTags = mkMap(mkEntry("client-id", CLIENT_ID));
    private final MetricName metricName1 =
        new MetricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, description1, clientLevelTags);
    private final MetricName metricName2 =
        new MetricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, description2, clientLevelTags);
    private final MockTime time = new MockTime(0);
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);

    private static MetricConfig eqMetricConfig(final MetricConfig metricConfig) {
        EasyMock.reportMatcher(new IArgumentMatcher() {
            private final StringBuffer message = new StringBuffer();

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof MetricConfig) {
                    final MetricConfig otherMetricConfig = (MetricConfig) argument;
                    final boolean equalsComparisons =
                        (otherMetricConfig.quota() == metricConfig.quota() ||
                        otherMetricConfig.quota().equals(metricConfig.quota())) &&
                        otherMetricConfig.tags().equals(metricConfig.tags());
                    if (otherMetricConfig.eventWindow() == metricConfig.eventWindow() &&
                        otherMetricConfig.recordLevel() == metricConfig.recordLevel() &&
                        equalsComparisons &&
                        otherMetricConfig.samples() == metricConfig.samples() &&
                        otherMetricConfig.timeWindowMs() == metricConfig.timeWindowMs()) {

                        return true;
                    } else {
                        message.append("{ ");
                        message.append("eventWindow=");
                        message.append(otherMetricConfig.eventWindow());
                        message.append(", ");
                        message.append("recordLevel=");
                        message.append(otherMetricConfig.recordLevel());
                        message.append(", ");
                        message.append("quota=");
                        message.append(otherMetricConfig.quota().toString());
                        message.append(", ");
                        message.append("samples=");
                        message.append(otherMetricConfig.samples());
                        message.append(", ");
                        message.append("tags=");
                        message.append(otherMetricConfig.tags().toString());
                        message.append(", ");
                        message.append("timeWindowMs=");
                        message.append(otherMetricConfig.timeWindowMs());
                        message.append(" }");
                    }
                }
                message.append("not a MetricConfig object");
                return false;
            }

            @Override
            public void appendTo(final StringBuffer buffer) {
                buffer.append(message);
            }
        });
        return null;
    }


    private void addSensorsOnAllLevels(final Metrics metrics, final StreamsMetricsImpl streamsMetrics) {
        expect(metrics.sensor(anyString(), anyObject(RecordingLevel.class), anyObject(Sensor[].class)))
            .andStubReturn(sensor);
        expect(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, description1, clientLevelTags))
            .andReturn(metricName1);
        expect(metrics.metricName(METRIC_NAME2, CLIENT_LEVEL_GROUP, description2, clientLevelTags))
            .andReturn(metricName2);
        replay(metrics);
        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME1, description1, RecordingLevel.INFO, "value");
        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME2, description2, RecordingLevel.INFO, "value");
        streamsMetrics.threadLevelSensor(THREAD_ID, sensorName1, RecordingLevel.INFO);
        streamsMetrics.threadLevelSensor(THREAD_ID, sensorName2, RecordingLevel.INFO);
        streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, sensorName1, RecordingLevel.INFO);
        streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, sensorName2, RecordingLevel.INFO);
        streamsMetrics.storeLevelSensor(THREAD_ID, TASK_ID, storeName, sensorName1, RecordingLevel.INFO);
        streamsMetrics.storeLevelSensor(THREAD_ID, TASK_ID, storeName, sensorName2, RecordingLevel.INFO);
    }

    private void setupGetSensorTest(final Metrics metrics,
                                    final String level,
                                    final RecordingLevel recordingLevel) {
        final String fullSensorName =
            INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + level + SENSOR_NAME_DELIMITER + sensorName1;
        final Sensor[] parents = {};
        expect(metrics.sensor(fullSensorName, recordingLevel, parents)).andReturn(sensor);
        replay(metrics);
    }

    @Test
    public void shouldAddClientLevelImmutableMetric() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        final String value = "immutable-value";
        final ImmutableMetricValue immutableValue = new ImmutableMetricValue<>(value);
        expect(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, description1, clientLevelTags))
            .andReturn(metricName1);
        metrics.addMetric(eq(metricName1), eqMetricConfig(metricConfig), eq(immutableValue));
        replay(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);

        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME1, description1, recordingLevel, value);

        verify(metrics);
    }

    @Test
    public void shouldAddClientLevelMutableMetric() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        final Gauge<String> valueProvider = (config, now) -> "mutable-value";
        expect(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, description1, clientLevelTags))
            .andReturn(metricName1);
        metrics.addMetric(eq(metricName1), eqMetricConfig(metricConfig), eq(valueProvider));
        replay(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);

        streamsMetrics.addClientLevelMutableMetric(METRIC_NAME1, description1, recordingLevel, valueProvider);

        verify(metrics);
    }

    @Test
    public void shouldGetThreadLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetSensorTest(metrics, THREAD_ID, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);

        final Sensor actualSensor = streamsMetrics.threadLevelSensor(THREAD_ID, sensorName1, recordingLevel);

        verify(metrics);
        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    private void setupRemoveSensorsTest(final Metrics metrics,
                                        final String level,
                                        final RecordingLevel recordingLevel) {
        final String fullSensorNamePrefix = INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + level + SENSOR_NAME_DELIMITER;
        final Sensor[] parents = {};
        resetToDefault(metrics);
        metrics.removeSensor(fullSensorNamePrefix + sensorName1);
        metrics.removeSensor(fullSensorNamePrefix + sensorName2);
        replay(metrics);
    }

    @Test
    public void shouldRemoveClientLevelMetrics() {
        final Metrics metrics = niceMock(Metrics.class);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);
        addSensorsOnAllLevels(metrics, streamsMetrics);
        resetToDefault(metrics);
        expect(metrics.removeMetric(metricName1)).andStubReturn(null);
        expect(metrics.removeMetric(metricName2)).andStubReturn(null);
        replay(metrics);

        streamsMetrics.removeAllClientLevelMetrics();

        verify(metrics);
    }

    @Test
    public void shouldRemoveThreadLevelSensors() {
        final Metrics metrics = niceMock(Metrics.class);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION);
        addSensorsOnAllLevels(metrics, streamsMetrics);
        setupRemoveSensorsTest(metrics, THREAD_ID, RecordingLevel.INFO);

        streamsMetrics.removeAllThreadLevelSensors(THREAD_ID);

        verify(metrics);
    }

    @Test(expected = NullPointerException.class)
    public void testNullMetrics() {
        new StreamsMetricsImpl(null, "", VERSION);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullSensor() {
        streamsMetrics.removeSensor(null);
    }

    @Test
    public void testRemoveSensor() {
        final String sensorName = "sensor1";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

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
    public void testMultiLevelSensorRemoval() {
        final Metrics registry = new Metrics();
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, THREAD_ID, VERSION);
        for (final MetricName defaultMetric : registry.metrics().keySet()) {
            registry.removeMetric(defaultMetric);
        }

        final String taskName = "taskName";
        final String operation = "operation";
        final Map<String, String> taskTags = mkMap(mkEntry("tkey", "value"));

        final String processorNodeName = "processorNodeName";
        final Map<String, String> nodeTags = mkMap(mkEntry("nkey", "value"));

        final Sensor parent1 = metrics.taskLevelSensor(THREAD_ID, taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent1, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        final int numberOfTaskMetrics = registry.metrics().size();

        final Sensor sensor1 = metrics.nodeLevelSensor(THREAD_ID, taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent1);
        addAvgAndMaxLatencyToSensor(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor1, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(THREAD_ID, taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor parent2 = metrics.taskLevelSensor(THREAD_ID, taskName, operation, Sensor.RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent2, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation, "", "");

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor sensor2 = metrics.nodeLevelSensor(THREAD_ID, taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent2);
        addAvgAndMaxLatencyToSensor(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor2, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(THREAD_ID, taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        metrics.removeAllTaskLevelSensors(THREAD_ID, taskName);

        assertThat(registry.metrics().size(), equalTo(0));
    }

    @Test
    public void testLatencyMetrics() {
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
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "", VERSION);

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
            Thread.currentThread().getName(),
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
    public void shouldGetClientLevelTagMap() {
        final Map<String, String> tagMap = streamsMetrics.clientLevelTagMap();

        assertThat(tagMap.size(), equalTo(1));
        assertThat(tagMap.get(StreamsMetricsImpl.CLIENT_ID_TAG), equalTo(CLIENT_ID));
    }

    @Test
    public void shouldGetStoreLevelTagMap() {
        final String taskName = "test-task";
        final String storeType = "remote-window";
        final String storeName = "window-keeper";

        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(THREAD_ID, taskName, storeType, storeName);

        assertThat(tagMap.size(), equalTo(3));
        assertThat(tagMap.get(StreamsMetricsImpl.THREAD_ID_TAG_0100_TO_23), equalTo(THREAD_ID));
        assertThat(tagMap.get(StreamsMetricsImpl.TASK_ID_TAG), equalTo(taskName));
        assertThat(tagMap.get(storeType + "-" + StreamsMetricsImpl.STORE_ID_TAG), equalTo(storeName));
    }

    @Test
    public void shouldGetCacheLevelTagMapForBuiltInMetricsLatestVersion() {
        shouldGetCacheLevelTagMap(StreamsMetricsImpl.METRICS_LATEST);
    }

    @Test
    public void shouldGetCacheLevelTagMapForBuiltInMetricsVersion0100To23() {
        shouldGetCacheLevelTagMap(StreamsMetricsImpl.METRICS_0100_TO_23);
    }

    private void shouldGetCacheLevelTagMap(final String builtInMetricsVersion) {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, builtInMetricsVersion);
        final String taskName = "taskName";
        final String storeName = "storeName";

        final Map<String, String> tagMap = streamsMetrics.cacheLevelTagMap(THREAD_ID, taskName, storeName);

        assertThat(tagMap.size(), equalTo(3));
        assertThat(
            tagMap.get(
                builtInMetricsVersion.equals(StreamsMetricsImpl.METRICS_LATEST) ? StreamsMetricsImpl.THREAD_ID_TAG
                    : StreamsMetricsImpl.THREAD_ID_TAG_0100_TO_23),
            equalTo(THREAD_ID)
        );
        assertThat(tagMap.get(StreamsMetricsImpl.TASK_ID_TAG), equalTo(taskName));
        assertThat(tagMap.get(StreamsMetricsImpl.RECORD_CACHE_ID_TAG), equalTo(storeName));
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

    @Test
    public void shouldAddAvgAndMinAndMaxMetricsToSensor() {
        StreamsMetricsImpl
            .addAvgAndMinAndMaxToSensor(sensor, group, tags, metricNamePrefix, description1, description2, description3);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedAvgMetricValue = (valueToRecord1 + valueToRecord2) / 2;
        verifyMetric(metricNamePrefix + "-avg", description1, valueToRecord1, valueToRecord2, expectedAvgMetricValue);
        verifyMetric(metricNamePrefix + "-min", description2, valueToRecord1, valueToRecord2, valueToRecord1);
        verifyMetric(metricNamePrefix + "-max", description3, valueToRecord1, valueToRecord2, valueToRecord2);
        assertThat(metrics.metrics().size(), equalTo(3 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldReturnMetricsVersionCurrent() {
        assertThat(
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsMetricsImpl.METRICS_LATEST).version(),
            equalTo(Version.LATEST)
        );
    }

    @Test
    public void shouldReturnMetricsVersionFrom100To23() {
        assertThat(
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsMetricsImpl.METRICS_0100_TO_23).version(),
            equalTo(Version.FROM_100_TO_23)
        );
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
