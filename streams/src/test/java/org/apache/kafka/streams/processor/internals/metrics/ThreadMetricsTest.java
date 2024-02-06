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

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.StreamThreadTotalBlockedTime;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ThreadMetricsTest {

    private static final String THREAD_ID = "thread-id";
    private static final String THREAD_LEVEL_GROUP = "stream-thread-metrics";

    private final Sensor expectedSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");


    @Test
    public void shouldGetProcessRatioSensor() {
        final String operation = "process-ratio";
        final String ratioDescription = "The fraction of time the thread spent on processing active tasks";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.processRatioSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addValueMetricToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    ratioDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetProcessRecordsSensor() {
        final String operation = "process-records";
        final String avgDescription = "The average number of records processed within an iteration";
        final String maxDescription = "The maximum number of records processed within an iteration";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.processRecordsSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    avgDescription,
                    maxDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetProcessLatencySensor() {
        final String operationLatency = "process" + LATENCY_SUFFIX;
        final String avgLatencyDescription = "The average process latency";
        final String maxLatencyDescription = "The maximum process latency";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operationLatency, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.processLatencySensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operationLatency,
                    avgLatencyDescription,
                    maxLatencyDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetProcessRateSensor() {
        final String operation = "process";
        final String operationRate = "process" + RATE_SUFFIX;
        final String totalDescription = "The total number of calls to process";
        final String rateDescription = "The average per-second number of calls to process";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operationRate, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.processRateSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetPollRatioSensor() {
        final String operation = "poll-ratio";
        final String ratioDescription = "The fraction of time the thread spent on polling records from consumer";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.pollRatioSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addValueMetricToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    ratioDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetPollRecordsSensor() {
        final String operation = "poll-records";
        final String avgDescription = "The average number of records polled from consumer within an iteration";
        final String maxDescription = "The maximum number of records polled from consumer within an iteration";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.pollRecordsSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    avgDescription,
                    maxDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetPollSensor() {
        final String operation = "poll";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to poll";
        final String rateDescription = "The average per-second number of calls to poll";
        final String avgLatencyDescription = "The average poll latency";
        final String maxLatencyDescription = "The maximum poll latency";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.pollSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operationLatency,
                    avgLatencyDescription,
                    maxLatencyDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetCommitSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to commit";
        final String rateDescription = "The average per-second number of calls to commit";
        final String avgLatencyDescription = "The average commit latency";
        final String maxLatencyDescription = "The maximum commit latency";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.commitSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operationLatency,
                    avgLatencyDescription,
                    maxLatencyDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetCommitRatioSensor() {
        final String operation = "commit-ratio";
        final String ratioDescription = "The fraction of time the thread spent on committing all tasks";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.commitRatioSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addValueMetricToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    ratioDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String operation = "punctuate";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to punctuate";
        final String rateDescription = "The average per-second number of calls to punctuate";
        final String avgLatencyDescription = "The average punctuate latency";
        final String maxLatencyDescription = "The maximum punctuate latency";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.punctuateSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operationLatency,
                    avgLatencyDescription,
                    maxLatencyDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetPunctuateRatioSensor() {
        final String operation = "punctuate-ratio";
        final String ratioDescription = "The fraction of time the thread spent on punctuating active tasks";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.punctuateRatioSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addValueMetricToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    ratioDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetCreateTaskSensor() {
        final String operation = "task-created";
        final String totalDescription = "The total number of newly created tasks";
        final String rateDescription = "The average per-second number of newly created tasks";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.createTaskSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetCloseTaskSensor() {
        final String operation = "task-closed";
        final String totalDescription = "The total number of closed tasks";
        final String rateDescription = "The average per-second number of closed tasks";
        when(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = ThreadMetrics.closeTaskSensor(THREAD_ID, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    THREAD_LEVEL_GROUP,
                    tagMap,
                    operation,
                    rateDescription,
                    totalDescription
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldAddThreadStartTimeMetric() {
        // Given:
        final long startTime = 123L;

        // When:
        ThreadMetrics.addThreadStartTimeMetric(
            "bongo",
            streamsMetrics,
            startTime
        );

        // Then:
        verify(streamsMetrics).addThreadLevelImmutableMetric(
            "thread-start-time",
            "The time that the thread was started",
            "bongo",
            startTime
        );
    }

    @Test
    public void shouldAddTotalBlockedTimeMetric() {
        // Given:
        final double startTime = 123.45;
        final StreamThreadTotalBlockedTime blockedTime = mock(StreamThreadTotalBlockedTime.class);
        when(blockedTime.compute()).thenReturn(startTime);

        // When:
        ThreadMetrics.addThreadBlockedTimeMetric(
            "burger",
            blockedTime,
            streamsMetrics
        );

        // Then:
        final ArgumentCaptor<Gauge<Double>> captor = gaugeCaptor();
        verify(streamsMetrics).addThreadLevelMutableMetric(
            eq("blocked-time-ns-total"),
            eq("The total time the thread spent blocked on kafka in nanoseconds"),
            eq("burger"),
            captor.capture()
        );
        assertThat(captor.getValue().value(null, 678L), is(startTime));
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<Gauge<Double>> gaugeCaptor() {
        return ArgumentCaptor.forClass(Gauge.class);
    }
}
