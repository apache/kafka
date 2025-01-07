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
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ImmutableMetricValue;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CLIENT_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CLIENT_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESS_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOPIC_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxLatencyToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.hamcrest.CoreMatchers.equalToObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StreamsMetricsImplTest {

    private static final String SENSOR_PREFIX_DELIMITER = ".";
    private static final String SENSOR_NAME_DELIMITER = ".s.";
    private static final String SENSOR_NAME_1 = "sensor1";
    private static final String SENSOR_NAME_2 = "sensor2";
    private static final String INTERNAL_PREFIX = "internal";
    private static final String CLIENT_ID = "test-client";
    private static final String PROCESS_ID = "test-process";
    private static final String THREAD_ID1 = "test-thread-1";
    private static final String TASK_ID1 = "test-task-1";
    private static final String TASK_ID2 = "test-task-2";
    private static final String NODE_ID1 = "test-node-1";
    private static final String NODE_ID2 = "test-node-2";
    private static final String TOPIC_ID1 = "test-topic-1";
    private static final String TOPIC_ID2 = "test-topic-2";
    private static final String METRIC_NAME1 = "test-metric1";
    private static final String METRIC_NAME2 = "test-metric2";
    private static final String THREAD_ID_TAG = "thread-id";
    private static final String TASK_ID_TAG = "task-id";
    private static final String SCOPE_NAME = "test-scope";
    private static final String STORE_ID_TAG = "-state-id";
    private static final String STORE_NAME1 = "store1";
    private static final String STORE_NAME2 = "store2";
    private static final Map<String, String> STORE_LEVEL_TAG_MAP = mkMap(
        mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()),
        mkEntry(TASK_ID_TAG, TASK_ID1),
        mkEntry(SCOPE_NAME + STORE_ID_TAG, STORE_NAME1)
    );
    private static final String RECORD_CACHE_ID_TAG = "record-cache-id";
    private static final String ENTITY_NAME = "test-entity";
    private static final String OPERATION_NAME = "test-operation";
    private static final String CUSTOM_TAG_KEY1 = "test-key1";
    private static final String CUSTOM_TAG_VALUE1 = "test-value1";
    private static final String CUSTOM_TAG_KEY2 = "test-key2";
    private static final String CUSTOM_TAG_VALUE2 = "test-value2";
    private static final RecordingLevel INFO_RECORDING_LEVEL = RecordingLevel.INFO;
    private static final String DESCRIPTION1 = "description number one";
    private static final String DESCRIPTION2 = "description number two";
    private static final String DESCRIPTION3 = "description number three";
    private static final Gauge<String> VALUE_PROVIDER = (config, now) -> "mutable-value";

    private final Metrics metrics = new Metrics();
    private final Sensor sensor = metrics.sensor("dummy");
    private final String metricNamePrefix = "metric";
    private final String group = "group";
    private final Map<String, String> tags = mkMap(mkEntry("tag", "value"));
    private final Map<String, String> clientLevelTags = mkMap(mkEntry(CLIENT_ID_TAG, CLIENT_ID), mkEntry(PROCESS_ID_TAG, PROCESS_ID));
    private final MetricName metricName1 =
        new MetricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, DESCRIPTION1, clientLevelTags);
    private final MetricName metricName2 =
        new MetricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, DESCRIPTION2, clientLevelTags);
    private final MockTime time = new MockTime(0);
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

    private static MetricConfig eqMetricConfig(final MetricConfig metricConfig) {
        final StringBuffer message = new StringBuffer();

        argThat((ArgumentMatcher<MetricConfig>) argument -> {
            if (argument instanceof MetricConfig) {
                final MetricConfig otherMetricConfig = argument;
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
        });
        return null;
    }

    private ArgumentCaptor<String> addSensorsOnAllLevels(final Metrics metrics, final StreamsMetricsImpl streamsMetrics) {
        final ArgumentCaptor<String> sensorKeys = ArgumentCaptor.forClass(String.class);
        final Sensor[] parents = {};
        when(metrics.sensor(sensorKeys.capture(), eq(INFO_RECORDING_LEVEL), parents))
            .thenReturn(sensor);
        when(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, DESCRIPTION1, clientLevelTags))
            .thenReturn(metricName1);
        when(metrics.metricName(METRIC_NAME2, CLIENT_LEVEL_GROUP, DESCRIPTION2, clientLevelTags))
            .thenReturn(metricName2);
        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME1, DESCRIPTION1, INFO_RECORDING_LEVEL, "value");
        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME2, DESCRIPTION2, INFO_RECORDING_LEVEL, "value");
        streamsMetrics.clientLevelSensor(SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.clientLevelSensor(SENSOR_NAME_2, INFO_RECORDING_LEVEL);
        streamsMetrics.threadLevelSensor(THREAD_ID1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.threadLevelSensor(THREAD_ID1, SENSOR_NAME_2, INFO_RECORDING_LEVEL);
        streamsMetrics.taskLevelSensor(THREAD_ID1, TASK_ID1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.taskLevelSensor(THREAD_ID1, TASK_ID1, SENSOR_NAME_2, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_2, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME2, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.addStoreLevelMutableMetric(
            TASK_ID1,
            SCOPE_NAME,
            STORE_NAME1,
            METRIC_NAME1,
            DESCRIPTION1,
            INFO_RECORDING_LEVEL,
            VALUE_PROVIDER
        );
        streamsMetrics.addStoreLevelMutableMetric(
            TASK_ID1,
            SCOPE_NAME,
            STORE_NAME1,
            METRIC_NAME2,
            DESCRIPTION2,
            INFO_RECORDING_LEVEL,
            VALUE_PROVIDER
        );
        streamsMetrics.addStoreLevelMutableMetric(
            TASK_ID1,
            SCOPE_NAME,
            STORE_NAME2,
            METRIC_NAME1,
            DESCRIPTION1,
            INFO_RECORDING_LEVEL,
            VALUE_PROVIDER
        );
        return sensorKeys;
    }

    private ArgumentCaptor<String> setupGetNewSensorTest(final Metrics metrics,
                                                  final RecordingLevel recordingLevel) {
        final ArgumentCaptor<String> sensorKey = ArgumentCaptor.forClass(String.class);
        when(metrics.getSensor(sensorKey.capture())).thenReturn(null);
        final Sensor[] parents = {};
        when(metrics.sensor(sensorKey.capture(), eq(INFO_RECORDING_LEVEL), parents)).thenReturn(sensor);
        return sensorKey;
    }

    private void setupGetExistingSensorTest(final Metrics metrics) {
        when(metrics.getSensor(anyString())).thenReturn(sensor);
    }

    @Test
    public void shouldGetNewThreadLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.threadLevelSensor(THREAD_ID1, SENSOR_NAME_1, recordingLevel);

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingThreadLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.threadLevelSensor(THREAD_ID1, SENSOR_NAME_1, recordingLevel);

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetNewTaskLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.taskLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingTaskLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.taskLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetNewTopicLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.topicLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            NODE_ID1,
            TOPIC_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingTopicLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.topicLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            NODE_ID1,
            TOPIC_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetNewStoreLevelSensorIfNoneExists() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final ArgumentCaptor<String> sensorKeys = setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.storeLevelSensor(
            TASK_ID1,
            STORE_NAME1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
        assertThat(sensorKeys.getAllValues().get(0), is(sensorKeys.getAllValues().get(1)));
    }

    @Test
    public void shouldGetExistingStoreLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.storeLevelSensor(
            TASK_ID1,
            STORE_NAME1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldUseSameStoreLevelSensorKeyWithTwoDifferentSensorNames() {
        final Metrics metrics = mock(Metrics.class);
        final ArgumentCaptor<String> sensorKeys = setUpSensorKeyTests(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_2, INFO_RECORDING_LEVEL);

        assertThat(sensorKeys.getAllValues().get(0), not(sensorKeys.getAllValues().get(1)));
    }

    @Test
    public void shouldNotUseSameStoreLevelSensorKeyWithDifferentTaskIds() {
        final Metrics metrics = mock(Metrics.class);
        final ArgumentCaptor<String> sensorKeys = setUpSensorKeyTests(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID2, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);

        assertThat(sensorKeys.getAllValues().get(0), not(sensorKeys.getAllValues().get(1)));
    }

    @Test
    public void shouldNotUseSameStoreLevelSensorKeyWithDifferentStoreNames() {
        final Metrics metrics = mock(Metrics.class);
        final ArgumentCaptor<String> sensorKeys = setUpSensorKeyTests(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME2, SENSOR_NAME_1, INFO_RECORDING_LEVEL);

        assertThat(sensorKeys.getAllValues().get(0), not(sensorKeys.getAllValues().get(1)));
    }

    @Test
    public void shouldNotUseSameStoreLevelSensorKeyWithDifferentThreadIds() throws InterruptedException {
        final Metrics metrics = mock(Metrics.class);
        final ArgumentCaptor<String> sensorKeys = setUpSensorKeyTests(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        final Thread otherThread =
            new Thread(() -> streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL));
        otherThread.start();
        otherThread.join();

        assertThat(sensorKeys.getAllValues().get(0), not(sensorKeys.getAllValues().get(1)));
    }

    @Test
    public void shouldUseSameStoreLevelSensorKeyWithSameSensorNames() {
        final Metrics metrics = mock(Metrics.class);
        final ArgumentCaptor<String> sensorKeys = setUpSensorKeyTests(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);
        streamsMetrics.storeLevelSensor(TASK_ID1, STORE_NAME1, SENSOR_NAME_1, INFO_RECORDING_LEVEL);

        assertThat(sensorKeys.getAllValues().get(0), is(sensorKeys.getAllValues().get(1)));
    }

    private ArgumentCaptor<String> setUpSensorKeyTests(final Metrics metrics) {
        final ArgumentCaptor<String> sensorKeys = ArgumentCaptor.forClass(String.class);
        when(metrics.getSensor(sensorKeys.capture())).thenReturn(sensor);
        return sensorKeys;
    }

    @Test
    public void shouldAddNewStoreLevelMutableMetric() {
        final Metrics metrics = mock(Metrics.class);
        final MetricName metricName =
            new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        final MetricConfig metricConfig = new MetricConfig().recordLevel(INFO_RECORDING_LEVEL);
        when(metrics.metricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP))
            .thenReturn(metricName);
        when(metrics.metric(metricName)).thenReturn(null);
        when(metrics.addMetricIfAbsent(eq(metricName), eqMetricConfig(metricConfig), eq(VALUE_PROVIDER))).thenReturn(null);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.addStoreLevelMutableMetric(
            TASK_ID1,
            SCOPE_NAME,
            STORE_NAME1,
            METRIC_NAME1,
            DESCRIPTION1,
            INFO_RECORDING_LEVEL,
            VALUE_PROVIDER
        );
    }

    @Test
    public void shouldCreateNewStoreLevelMutableMetric() {
        final MetricName metricName =
                new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        final MetricConfig metricConfig = new MetricConfig().recordLevel(INFO_RECORDING_LEVEL);
        final Metrics metrics = new Metrics(metricConfig);
        assertNull(metrics.metric(metricName));
        metrics.addMetricIfAbsent(metricName, metricConfig, VALUE_PROVIDER);
        assertNotNull(metrics.metric(metricName));
    }

    @Test
    public void shouldNotAddStoreLevelMutableMetricIfAlreadyExists() {
        final Metrics metrics = mock(Metrics.class);
        final MetricName metricName =
            new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        when(metrics.metricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP))
            .thenReturn(metricName);
        when(metrics.metric(metricName)).thenReturn(null);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.addStoreLevelMutableMetric(
            TASK_ID1,
            SCOPE_NAME,
            STORE_NAME1,
            METRIC_NAME1,
            DESCRIPTION1,
            INFO_RECORDING_LEVEL,
            VALUE_PROVIDER
        );
    }

    @Test
    public void shouldReturnSameMetricIfAlreadyCreated() {
        final MetricName metricName =
                new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        final MetricConfig metricConfig = new MetricConfig().recordLevel(INFO_RECORDING_LEVEL);
        final Metrics metrics = new Metrics(metricConfig);
        assertNull(metrics.metric(metricName));
        final KafkaMetric kafkaMetric = metrics.addMetricIfAbsent(metricName, metricConfig, VALUE_PROVIDER);
        assertEquals(kafkaMetric, metrics.addMetricIfAbsent(metricName, metricConfig, VALUE_PROVIDER));
    }

    @Test
    public void shouldCreateMetricOnceDuringConcurrentMetricCreationRequest() throws InterruptedException {
        final MetricName metricName =
                new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        final MetricConfig metricConfig = new MetricConfig().recordLevel(INFO_RECORDING_LEVEL);
        final Metrics metrics = new Metrics(metricConfig);
        assertNull(metrics.metric(metricName));
        final AtomicReference<KafkaMetric> metricCreatedViaThread1 = new AtomicReference<>();
        final AtomicReference<KafkaMetric> metricCreatedViaThread2 = new AtomicReference<>();

        final Thread thread1 = new Thread(() -> metricCreatedViaThread1.set(metrics.addMetricIfAbsent(metricName, metricConfig, VALUE_PROVIDER)));
        final Thread thread2 = new Thread(() -> metricCreatedViaThread2.set(metrics.addMetricIfAbsent(metricName, metricConfig, VALUE_PROVIDER)));

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
        assertEquals(metricCreatedViaThread1.get(), metricCreatedViaThread2.get());
    }

    @Test
    public void shouldRemoveStateStoreLevelSensors() {
        final Metrics metrics = mock(Metrics.class);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);
        final MetricName metricName1 =
            new MetricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP);
        final MetricName metricName2 =
            new MetricName(METRIC_NAME2, STATE_STORE_LEVEL_GROUP, DESCRIPTION2, STORE_LEVEL_TAG_MAP);
        when(metrics.metricName(METRIC_NAME1, STATE_STORE_LEVEL_GROUP, DESCRIPTION1, STORE_LEVEL_TAG_MAP))
            .thenReturn(metricName1);
        when(metrics.metricName(METRIC_NAME2, STATE_STORE_LEVEL_GROUP, DESCRIPTION2, STORE_LEVEL_TAG_MAP))
            .thenReturn(metricName2);
        final ArgumentCaptor<String> sensorKeys = addSensorsOnAllLevels(metrics, streamsMetrics);
        doNothing().when(metrics).removeSensor(sensorKeys.getAllValues().get(6));
        doNothing().when(metrics).removeSensor(sensorKeys.getAllValues().get(7));
        when(metrics.removeMetric(metricName1)).thenReturn(null);
        when(metrics.removeMetric(metricName2)).thenReturn(null);

        streamsMetrics.removeAllStoreLevelSensorsAndMetrics(TASK_ID1, STORE_NAME1);
    }

    @Test
    public void shouldGetNewNodeLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.nodeLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            NODE_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingNodeLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.nodeLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            NODE_ID1,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetNewCacheLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final String processorCacheName = "processorNodeName";
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.cacheLevelSensor(
            THREAD_ID1,
            TASK_ID1,
            processorCacheName,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingCacheLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final String processorCacheName = "processorNodeName";
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.cacheLevelSensor(
            THREAD_ID1, TASK_ID1,
            processorCacheName,
            SENSOR_NAME_1,
            recordingLevel
        );

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetNewClientLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetNewSensorTest(metrics, recordingLevel);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.clientLevelSensor(SENSOR_NAME_1, recordingLevel);

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldGetExistingClientLevelSensor() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        setupGetExistingSensorTest(metrics);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        final Sensor actualSensor = streamsMetrics.clientLevelSensor(SENSOR_NAME_1, recordingLevel);

        assertThat(actualSensor, is(equalToObject(sensor)));
    }

    @Test
    public void shouldAddClientLevelImmutableMetric() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        final String value = "immutable-value";
        final ImmutableMetricValue immutableValue = new ImmutableMetricValue<>(value);
        when(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, DESCRIPTION1, clientLevelTags))
            .thenReturn(metricName1);
        doNothing().when(metrics).addMetric(eq(metricName1), eqMetricConfig(metricConfig), eq(immutableValue));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.addClientLevelImmutableMetric(METRIC_NAME1, DESCRIPTION1, recordingLevel, value);
    }

    @Test
    public void shouldAddClientLevelMutableMetric() {
        final Metrics metrics = mock(Metrics.class);
        final RecordingLevel recordingLevel = RecordingLevel.INFO;
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        final Gauge<String> valueProvider = (config, now) -> "mutable-value";
        when(metrics.metricName(METRIC_NAME1, CLIENT_LEVEL_GROUP, DESCRIPTION1, clientLevelTags))
            .thenReturn(metricName1);
        doNothing().when(metrics).addMetric(eq(metricName1), eqMetricConfig(metricConfig), eq(valueProvider));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);

        streamsMetrics.addClientLevelMutableMetric(METRIC_NAME1, DESCRIPTION1, recordingLevel, valueProvider);
    }

    @Test
    public void shouldProvideCorrectStrings() {
        assertThat(LATENCY_SUFFIX, is("-latency"));
        assertThat(ROLLUP_VALUE, is("all"));
    }

    private void setupRemoveSensorsTest(final Metrics metrics,
                                        final String level) {
        final String fullSensorNamePrefix = INTERNAL_PREFIX + SENSOR_PREFIX_DELIMITER + level + SENSOR_NAME_DELIMITER;
        doNothing().when(metrics).removeSensor(fullSensorNamePrefix + SENSOR_NAME_1);
        doNothing().when(metrics).removeSensor(fullSensorNamePrefix + SENSOR_NAME_2);
    }

    @Test
    public void shouldRemoveClientLevelMetricsAndSensors() {
        final Metrics metrics = mock(Metrics.class);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);
        final ArgumentCaptor<String> sensorKeys = addSensorsOnAllLevels(metrics, streamsMetrics);

        doNothing().when(metrics).removeSensor(sensorKeys.getAllValues().get(0));
        doNothing().when(metrics).removeSensor(sensorKeys.getAllValues().get(1));
        when(metrics.removeMetric(metricName1)).thenReturn(null);
        when(metrics.removeMetric(metricName2)).thenReturn(null);
        streamsMetrics.removeAllClientLevelSensorsAndMetrics();
    }

    @Test
    public void shouldRemoveThreadLevelSensors() {
        final Metrics metrics = mock(Metrics.class);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);
        addSensorsOnAllLevels(metrics, streamsMetrics);
        setupRemoveSensorsTest(metrics, THREAD_ID1);

        streamsMetrics.removeAllThreadLevelSensors(THREAD_ID1);
    }

    @Test
    public void testNullMetrics() {
        assertThrows(NullPointerException.class, () -> new StreamsMetricsImpl(null, "", PROCESS_ID, time));
    }

    @Test
    public void testRemoveNullSensor() {
        assertThrows(NullPointerException.class, () -> streamsMetrics.removeSensor(null));
    }

    @Test
    public void testRemoveSensor() {
        final String sensorName = "sensor1";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addSensor(sensorName, RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        final Sensor sensor1a = streamsMetrics.addSensor(sensorName, RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        final Sensor sensor2 = streamsMetrics.addLatencyRateTotalSensor(scope, entity, operation, RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        final Sensor sensor3 = streamsMetrics.addRateTotalSensor(scope, entity, operation, RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);

        assertEquals(Collections.emptyMap(), streamsMetrics.parentSensors());
    }

    @Test
    public void testMultiLevelSensorRemoval() {
        final Metrics registry = new Metrics();
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, THREAD_ID1, PROCESS_ID, time);
        for (final MetricName defaultMetric : registry.metrics().keySet()) {
            registry.removeMetric(defaultMetric);
        }

        final String taskName = "taskName";
        final String operation = "operation";
        final Map<String, String> taskTags = mkMap(mkEntry("tkey", "value"));

        final String processorNodeName = "processorNodeName";
        final Map<String, String> nodeTags = mkMap(mkEntry("nkey", "value"));

        final String topicName = "topicName";
        final Map<String, String> topicTags = mkMap(mkEntry("tkey", "value"));

        final Sensor parent1 = metrics.taskLevelSensor(THREAD_ID1, taskName, operation, RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent1, PROCESSOR_NODE_LEVEL_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent1, PROCESSOR_NODE_LEVEL_GROUP, taskTags, operation, "", "");

        final int numberOfTaskMetrics = registry.metrics().size();

        final Sensor sensor1 = metrics.nodeLevelSensor(THREAD_ID1, taskName, processorNodeName, operation, RecordingLevel.DEBUG, parent1);
        addAvgAndMaxLatencyToSensor(sensor1, PROCESSOR_NODE_LEVEL_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor1, PROCESSOR_NODE_LEVEL_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        final int numberOfNodeMetrics = registry.metrics().size();

        final Sensor child1 = metrics.topicLevelSensor(THREAD_ID1, taskName, processorNodeName, topicName, operation, RecordingLevel.DEBUG, sensor1);
        addAvgAndMaxLatencyToSensor(child1, TOPIC_LEVEL_GROUP, topicTags, operation);
        addInvocationRateAndCountToSensor(child1, TOPIC_LEVEL_GROUP, topicTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfNodeMetrics));

        metrics.removeAllTopicLevelSensors(THREAD_ID1, taskName, processorNodeName, topicName);

        assertThat(registry.metrics().size(), equalTo(numberOfNodeMetrics));

        metrics.removeAllNodeLevelSensors(THREAD_ID1, taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor parent2 = metrics.taskLevelSensor(THREAD_ID1, taskName, operation, RecordingLevel.DEBUG);
        addAvgAndMaxLatencyToSensor(parent2, PROCESSOR_NODE_LEVEL_GROUP, taskTags, operation);
        addInvocationRateAndCountToSensor(parent2, PROCESSOR_NODE_LEVEL_GROUP, taskTags, operation, "", "");

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        final Sensor sensor2 = metrics.nodeLevelSensor(THREAD_ID1, taskName, processorNodeName, operation, RecordingLevel.DEBUG, parent2);
        addAvgAndMaxLatencyToSensor(sensor2, PROCESSOR_NODE_LEVEL_GROUP, nodeTags, operation);
        addInvocationRateAndCountToSensor(sensor2, PROCESSOR_NODE_LEVEL_GROUP, nodeTags, operation, "", "");

        assertThat(registry.metrics().size(), greaterThan(numberOfTaskMetrics));

        metrics.removeAllNodeLevelSensors(THREAD_ID1, taskName, processorNodeName);

        assertThat(registry.metrics().size(), equalTo(numberOfTaskMetrics));

        metrics.removeAllTaskLevelSensors(THREAD_ID1, taskName);

        assertThat(registry.metrics().size(), equalTo(0));
    }

    @Test
    public void testLatencyMetrics() {
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addLatencyRateTotalSensor(scope, entity, operation, RecordingLevel.DEBUG);

        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        final int otherMetricsCount = 2; // Latency-max and Latency-avg
        // 2 meters and 2 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(defaultMetrics + meterMetricsCount + otherMetricsCount, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }

    @Test
    public void testThroughputMetrics() {
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addRateTotalSensor(scope, entity, operation, RecordingLevel.DEBUG);

        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(defaultMetrics + meterMetricsCount, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }

    @Test
    public void testTotalMetricDoesntDecrease() {
        final MockTime time = new MockTime(1);
        final MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS);
        final Metrics metrics = new Metrics(config, time);
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "", PROCESS_ID, time);

        final String scope = "scope";
        final String entity = "entity";
        final String operation = "op";

        final Sensor sensor = streamsMetrics.addLatencyRateTotalSensor(
            scope,
            entity,
            operation,
            RecordingLevel.INFO
        );

        final double latency = 100.0;
        final MetricName totalMetricName = metrics.metricName(
            "op-total",
            "stream-scope-metrics",
            "",
            "thread-id",
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
    public void shouldAddLatencyRateTotalSensor() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);
        shouldAddCustomSensor(
            streamsMetrics.addLatencyRateTotalSensor(SCOPE_NAME, ENTITY_NAME, OPERATION_NAME, RecordingLevel.DEBUG),
            streamsMetrics,
            Arrays.asList(
                OPERATION_NAME + LATENCY_SUFFIX + AVG_SUFFIX,
                OPERATION_NAME + LATENCY_SUFFIX + MAX_SUFFIX,
                OPERATION_NAME + TOTAL_SUFFIX,
                OPERATION_NAME + RATE_SUFFIX
            )
        );
    }

    @Test
    public void shouldAddRateTotalSensor() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, PROCESS_ID, time);
        shouldAddCustomSensor(
            streamsMetrics.addRateTotalSensor(SCOPE_NAME, ENTITY_NAME, OPERATION_NAME, RecordingLevel.DEBUG),
            streamsMetrics,
            Arrays.asList(OPERATION_NAME + TOTAL_SUFFIX, OPERATION_NAME + RATE_SUFFIX)
        );
    }

    @Test
    public void shouldAddLatencyRateTotalSensorWithCustomTags() {
        final Sensor sensor = streamsMetrics.addLatencyRateTotalSensor(
            SCOPE_NAME,
            ENTITY_NAME,
            OPERATION_NAME,
            RecordingLevel.DEBUG,
            CUSTOM_TAG_KEY1,
            CUSTOM_TAG_VALUE1,
            CUSTOM_TAG_KEY2,
            CUSTOM_TAG_VALUE2
        );
        final Map<String, String> tags = customTags(streamsMetrics);
        shouldAddCustomSensorWithTags(
            sensor,
            Arrays.asList(
                OPERATION_NAME + LATENCY_SUFFIX + AVG_SUFFIX,
                OPERATION_NAME + LATENCY_SUFFIX + MAX_SUFFIX,
                OPERATION_NAME + TOTAL_SUFFIX,
                OPERATION_NAME + RATE_SUFFIX
            ),
            tags
        );
    }

    @Test
    public void shouldAddRateTotalSensorWithCustomTags() {
        final Sensor sensor = streamsMetrics.addRateTotalSensor(
            SCOPE_NAME,
            ENTITY_NAME,
            OPERATION_NAME,
            RecordingLevel.DEBUG,
            CUSTOM_TAG_KEY1,
            CUSTOM_TAG_VALUE1,
            CUSTOM_TAG_KEY2,
            CUSTOM_TAG_VALUE2
        );
        final Map<String, String> tags = customTags(streamsMetrics);
        shouldAddCustomSensorWithTags(
            sensor,
            Arrays.asList(
                OPERATION_NAME + TOTAL_SUFFIX,
                OPERATION_NAME + RATE_SUFFIX
            ),
            tags
        );
    }

    private void shouldAddCustomSensor(final Sensor sensor,
                                       final StreamsMetricsImpl streamsMetrics,
                                       final List<String> metricsNames) {
        final Map<String, String> tags = tags(streamsMetrics);
        shouldAddCustomSensorWithTags(sensor, metricsNames, tags);
    }

    private void shouldAddCustomSensorWithTags(final Sensor sensor,
                                               final List<String> metricsNames,
                                               final Map<String, String> tags) {
        final String group = "stream-" + SCOPE_NAME + "-metrics";
        assertTrue(sensor.hasMetrics());
        assertThat(
            sensor.name(),
            is("external." + Thread.currentThread().getName() + ".entity." + ENTITY_NAME + ".s." + OPERATION_NAME)
        );
        for (final String name : metricsNames) {
            assertTrue(StreamsTestUtils.containsMetric(metrics, name, group, tags));
        }
    }

    private Map<String, String> tags(final StreamsMetricsImpl streamsMetrics) {
        return mkMap(
            mkEntry(
                streamsMetrics.version() == Version.LATEST ? THREAD_ID_TAG : CLIENT_ID_TAG,
                Thread.currentThread().getName()
            ),
            mkEntry(SCOPE_NAME + "-id", ENTITY_NAME)
        );
    }

    private Map<String, String> customTags(final StreamsMetricsImpl streamsMetrics) {
        final Map<String, String> tags = tags(streamsMetrics);
        tags.put(CUSTOM_TAG_KEY1, CUSTOM_TAG_VALUE1);
        tags.put(CUSTOM_TAG_KEY2, CUSTOM_TAG_VALUE2);
        return tags;
    }

    @Test
    public void shouldThrowIfLatencyRateTotalSensorIsAddedWithOddTags() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> streamsMetrics.addLatencyRateTotalSensor(
                SCOPE_NAME,
                ENTITY_NAME,
                OPERATION_NAME,
                RecordingLevel.DEBUG,
                "bad-tag")
        );
        assertThat(exception.getMessage(), is("Tags needs to be specified in key-value pairs"));
    }

    @Test
    public void shouldThrowIfRateTotalSensorIsAddedWithOddTags() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> streamsMetrics.addRateTotalSensor(
                SCOPE_NAME,
                ENTITY_NAME,
                OPERATION_NAME,
                RecordingLevel.DEBUG,
                "bad-tag")
        );
        assertThat(exception.getMessage(), is("Tags needs to be specified in key-value pairs"));
    }

    @Test
    public void shouldGetClientLevelTagMap() {
        final Map<String, String> tagMap = streamsMetrics.clientLevelTagMap();

        assertThat(tagMap.size(), equalTo(2));
        assertThat(tagMap.get(StreamsMetricsImpl.CLIENT_ID_TAG), equalTo(CLIENT_ID));
        assertThat(tagMap.get(StreamsMetricsImpl.PROCESS_ID_TAG), equalTo(PROCESS_ID));
    }

    @Test
    public void shouldGetStoreLevelTagMap() {
        final String taskName = "test-task";
        final String storeType = "remote-window";
        final String storeName = "window-keeper";
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);

        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskName, storeType, storeName);

        assertThat(tagMap.size(), equalTo(3));
        assertThat(
            tagMap.get(StreamsMetricsImpl.THREAD_ID_TAG),
            equalTo(Thread.currentThread().getName()));
        assertThat(tagMap.get(StreamsMetricsImpl.TASK_ID_TAG), equalTo(taskName));
        assertThat(tagMap.get(storeType + "-" + StreamsMetricsImpl.STORE_ID_TAG), equalTo(storeName));
    }

    @Test
    public void shouldGetCacheLevelTagMap() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);
        final String taskName = "taskName";
        final String storeName = "storeName";

        final Map<String, String> tagMap = streamsMetrics.cacheLevelTagMap(THREAD_ID1, taskName, storeName);

        assertThat(tagMap.size(), equalTo(3));
        assertThat(
            tagMap.get(StreamsMetricsImpl.THREAD_ID_TAG),
            equalTo(THREAD_ID1)
        );
        assertThat(tagMap.get(TASK_ID_TAG), equalTo(taskName));
        assertThat(tagMap.get(RECORD_CACHE_ID_TAG), equalTo(storeName));
    }

    @Test
    public void shouldGetThreadLevelTagMap() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);

        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(THREAD_ID1);

        assertThat(tagMap.size(), equalTo(1));
        assertThat(
            tagMap.get(THREAD_ID_TAG),
            equalTo(THREAD_ID1)
        );
    }

    @Test
    public void shouldAddInvocationRateToSensor() {
        final Sensor sensor = mock(Sensor.class);
        final MetricName expectedMetricName = new MetricName(METRIC_NAME1 + "-rate", group, DESCRIPTION1, tags);
        when(sensor.add(eq(expectedMetricName), any(Rate.class))).thenReturn(true);

        StreamsMetricsImpl.addInvocationRateToSensor(sensor, group, tags, METRIC_NAME1, DESCRIPTION1);
    }

    @Test
    public void shouldAddAmountRateAndSum() {
        StreamsMetricsImpl
            .addRateOfSumAndSumMetricsToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1, DESCRIPTION2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 72.0;
        final long defaultWindowSizeInSeconds = Duration.ofMillis(new MetricConfig().timeWindowMs()).getSeconds();
        final double expectedRateMetricValue = (valueToRecord1 + valueToRecord2) / defaultWindowSizeInSeconds;
        verifyMetric(metricNamePrefix + "-rate", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedRateMetricValue);
        final double expectedSumMetricValue = 2 * valueToRecord1 + 2 * valueToRecord2; // values are recorded once for each metric verification
        verifyMetric(metricNamePrefix + "-total", DESCRIPTION2, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddSum() {
        StreamsMetricsImpl.addSumMetricToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedSumMetricValue = valueToRecord1 + valueToRecord2;
        verifyMetric(metricNamePrefix + "-total", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(1 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddAmountRate() {
        StreamsMetricsImpl.addRateOfSumMetricToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 72.0;
        final long defaultWindowSizeInSeconds = Duration.ofMillis(new MetricConfig().timeWindowMs()).getSeconds();
        final double expectedRateMetricValue = (valueToRecord1 + valueToRecord2) / defaultWindowSizeInSeconds;
        verifyMetric(metricNamePrefix + "-rate", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedRateMetricValue);
        assertThat(metrics.metrics().size(), equalTo(1 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddValue() {
        StreamsMetricsImpl.addValueMetricToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1);

        final KafkaMetric ratioMetric = metrics.metric(new MetricName(metricNamePrefix, group, DESCRIPTION1, tags));
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
    public void shouldAddTotalCountAndSumMetricsToSensor() {
        final String totalMetricNamePrefix = "total";
        final String sumMetricNamePrefix = "count";
        StreamsMetricsImpl
            .addTotalCountAndSumMetricsToSensor(sensor, group, tags, totalMetricNamePrefix, sumMetricNamePrefix, DESCRIPTION1, DESCRIPTION2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedCountMetricValue = 2;
        verifyMetric(totalMetricNamePrefix + "-total", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedCountMetricValue);
        final double expectedSumMetricValue = 2 * valueToRecord1 + 2 * valueToRecord2; // values are recorded once for each metric verification
        verifyMetric(sumMetricNamePrefix + "-total", DESCRIPTION2, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddAvgAndTotalMetricsToSensor() {
        StreamsMetricsImpl
            .addAvgAndSumMetricsToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1, DESCRIPTION2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedAvgMetricValue = (valueToRecord1 + valueToRecord2) / 2;
        verifyMetric(metricNamePrefix + "-avg", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedAvgMetricValue);
        final double expectedSumMetricValue = 2 * valueToRecord1 + 2 * valueToRecord2; // values are recorded once for each metric verification
        verifyMetric(metricNamePrefix + "-total", DESCRIPTION2, valueToRecord1, valueToRecord2, expectedSumMetricValue);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddAvgAndMinAndMaxMetricsToSensor() {
        StreamsMetricsImpl
            .addAvgAndMinAndMaxToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1, DESCRIPTION2, DESCRIPTION3);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        final double expectedAvgMetricValue = (valueToRecord1 + valueToRecord2) / 2;
        verifyMetric(metricNamePrefix + "-avg", DESCRIPTION1, valueToRecord1, valueToRecord2, expectedAvgMetricValue);
        verifyMetric(metricNamePrefix + "-min", DESCRIPTION2, valueToRecord1, valueToRecord2, valueToRecord1);
        verifyMetric(metricNamePrefix + "-max", DESCRIPTION3, valueToRecord1, valueToRecord2, valueToRecord2);
        assertThat(metrics.metrics().size(), equalTo(3 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldAddMinAndMaxMetricsToSensor() {
        StreamsMetricsImpl
            .addMinAndMaxToSensor(sensor, group, tags, metricNamePrefix, DESCRIPTION1, DESCRIPTION2);

        final double valueToRecord1 = 18.0;
        final double valueToRecord2 = 42.0;
        verifyMetric(metricNamePrefix + "-min", DESCRIPTION1, valueToRecord1, valueToRecord2, valueToRecord1);
        verifyMetric(metricNamePrefix + "-max", DESCRIPTION2, valueToRecord1, valueToRecord2, valueToRecord2);
        assertThat(metrics.metrics().size(), equalTo(2 + 1)); // one metric is added automatically in the constructor of Metrics
    }

    @Test
    public void shouldReturnMetricsVersionCurrent() {
        assertThat(
            new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time).version(),
            equalTo(Version.LATEST)
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

    @Test
    public void shouldMeasureLatency() {
        final long startTime = 6;
        final long endTime = 10;
        final Sensor sensor = mock(Sensor.class);
        when(sensor.shouldRecord()).thenReturn(true);
        when(sensor.hasMetrics()).thenReturn(true);
        doNothing().when(sensor).record(endTime - startTime);
        final Time time = mock(Time.class);
        when(time.nanoseconds()).thenReturn(startTime).thenReturn(endTime);

        StreamsMetricsImpl.maybeMeasureLatency(() -> { }, time, sensor);
    }

    @Test
    public void shouldNotMeasureLatencyDueToRecordingLevel() {
        final Sensor sensor = mock(Sensor.class);
        when(sensor.shouldRecord()).thenReturn(false);
        final Time time = mock(Time.class);

        StreamsMetricsImpl.maybeMeasureLatency(() -> { }, time, sensor);
    }

    @Test
    public void shouldNotMeasureLatencyBecauseSensorHasNoMetrics() {
        final Sensor sensor = mock(Sensor.class);
        when(sensor.shouldRecord()).thenReturn(true);
        when(sensor.hasMetrics()).thenReturn(false);
        final Time time = mock(Time.class);

        StreamsMetricsImpl.maybeMeasureLatency(() -> { }, time, sensor);
    }

    @Test
    public void shouldAddThreadLevelMutableMetric() {
        final int measuredValue = 123;
        final StreamsMetricsImpl streamsMetrics
            = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);

        streamsMetrics.addThreadLevelMutableMetric(
            "foobar",
            "test metric",
            "t1",
            (c, t) -> measuredValue
        );

        final MetricName name = metrics.metricName(
            "foobar",
            THREAD_LEVEL_GROUP,
            Collections.singletonMap("thread-id", "t1")
        );
        assertThat(metrics.metric(name), notNullValue());
        assertThat(metrics.metric(name).metricValue(), equalTo(measuredValue));
    }

    @Test
    public void shouldCleanupThreadLevelMutableMetric() {
        final int measuredValue = 123;
        final StreamsMetricsImpl streamsMetrics
            = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);
        streamsMetrics.addThreadLevelMutableMetric(
            "foobar",
            "test metric",
            "t1",
            (c, t) -> measuredValue
        );

        streamsMetrics.removeAllThreadLevelMetrics("t1");

        final MetricName name = metrics.metricName(
            "foobar",
            THREAD_LEVEL_GROUP,
            Collections.singletonMap("thread-id", "t1")
        );
        assertThat(metrics.metric(name), nullValue());
    }

    @Test
    public void shouldAddThreadLevelImmutableMetric() {
        final int measuredValue = 123;
        final StreamsMetricsImpl streamsMetrics
            = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);

        streamsMetrics.addThreadLevelImmutableMetric(
            "foobar",
            "test metric",
            "t1",
            measuredValue
        );

        final MetricName name = metrics.metricName(
            "foobar",
            THREAD_LEVEL_GROUP,
            Collections.singletonMap("thread-id", "t1")
        );
        assertThat(metrics.metric(name), notNullValue());
        assertThat(metrics.metric(name).metricValue(), equalTo(measuredValue));
    }

    @Test
    public void shouldCleanupThreadLevelImmutableMetric() {
        final int measuredValue = 123;
        final StreamsMetricsImpl streamsMetrics
            = new StreamsMetricsImpl(metrics, THREAD_ID1, PROCESS_ID, time);
        streamsMetrics.addThreadLevelImmutableMetric(
            "foobar",
            "test metric",
            "t1",
            measuredValue
        );

        streamsMetrics.removeAllThreadLevelMetrics("t1");

        final MetricName name = metrics.metricName(
            "foobar",
            THREAD_LEVEL_GROUP,
            Collections.singletonMap("thread-id", "t1")
        );
        assertThat(metrics.metric(name), nullValue());
    }
}
