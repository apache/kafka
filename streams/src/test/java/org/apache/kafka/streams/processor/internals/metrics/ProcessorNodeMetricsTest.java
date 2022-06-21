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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;

import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProcessorNodeMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String PROCESSOR_NODE_ID = "test-processor";

    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");
    private final Map<String, String> parentTagMap = Collections.singletonMap("hi", "universe");

    private final Sensor expectedSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private static final MockedStatic<StreamsMetricsImpl> STREAMS_METRICS_STATIC_MOCK = mockStatic(StreamsMetricsImpl.class);
    private final Sensor expectedParentSensor = mock(Sensor.class);

    @AfterClass
    public static void cleanUp() {
        STREAMS_METRICS_STATIC_MOCK.close();
    }

    @Test
    public void shouldGetSuppressionEmitSensor() {
        final String metricNamePrefix = "suppression-emit";
        final String descriptionOfCount = "The total number of emitted records from the suppression buffer";
        final String descriptionOfRate = "The average number of emitted records from the suppression buffer per second";
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);

        final Sensor sensor = ProcessorNodeMetrics.suppressionEmitSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics);

        verifySensor(sensor, metricNamePrefix, descriptionOfRate, descriptionOfCount);
    }

    @Test
    public void shouldGetIdempotentUpdateSkipSensor() {
        final String metricNamePrefix = "idempotent-update-skip";
        final String descriptionOfCount = "The total number of skipped idempotent updates";
        final String descriptionOfRate = "The average number of skipped idempotent updates per second";
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);

        final Sensor sensor = ProcessorNodeMetrics.skippedIdempotentUpdatesSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics);

        verifySensor(sensor, metricNamePrefix, descriptionOfRate, descriptionOfCount);
    }

    @Test
    public void shouldGetProcessAtSourceSensor() {
        final String metricNamePrefix = "process";
        final String descriptionOfCount = "The total number of calls to process";
        final String descriptionOfRate = "The average number of calls to process per second";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .thenReturn(expectedParentSensor);
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID))
            .thenReturn(parentTagMap);
        setUpThroughputSensor(metricNamePrefix, RecordingLevel.DEBUG, expectedParentSensor);

        final Sensor sensor = ProcessorNodeMetrics.processAtSourceSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics);

        verifySensor(sensor, metricNamePrefix, descriptionOfRate, descriptionOfCount);
    }

    @Test
    public void shouldGetForwardSensor() {
        final String metricNamePrefix = "forward";
        final String descriptionOfCount = "The total number of calls to forward";
        final String descriptionOfRate = "The average number of calls to forward per second";
        setUpThroughputParentSensor(
            metricNamePrefix
        );
        setUpThroughputSensor(
            metricNamePrefix,
            RecordingLevel.DEBUG,
            expectedParentSensor
        );

        final Sensor sensor = ProcessorNodeMetrics.forwardSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics);

        verifySensor(sensor, metricNamePrefix, descriptionOfRate, descriptionOfCount);
        verifyParentSensor(metricNamePrefix, descriptionOfRate, descriptionOfCount);
    }

    private void setUpThroughputParentSensor(final String metricNamePrefix) {
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .thenReturn(expectedParentSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
            .thenReturn(parentTagMap);
    }

    private void setUpThroughputSensor(final String metricNamePrefix,
                                       final RecordingLevel recordingLevel,
                                       final Sensor... parentSensors) {
        when(streamsMetrics.nodeLevelSensor(
            THREAD_ID,
            TASK_ID,
            PROCESSOR_NODE_ID,
            metricNamePrefix,
            recordingLevel,
            parentSensors
        )).thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);
    }

    private void verifySensor(final Sensor sensor,
                              final String metricNamePrefix,
                              final String descriptionOfRate,
                              final String descriptionOfCount) {
        assertThat(sensor, is(expectedSensor));
        STREAMS_METRICS_STATIC_MOCK.verify(
            () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedSensor,
                PROCESSOR_NODE_LEVEL_GROUP,
                tagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
            )
        );
    }

    private void verifyParentSensor(final String metricNamePrefix,
                                    final String descriptionOfRate,
                                    final String descriptionOfCount) {
        STREAMS_METRICS_STATIC_MOCK.verify(
            () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedParentSensor,
                PROCESSOR_NODE_LEVEL_GROUP,
                parentTagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
            )
        );
    }
}