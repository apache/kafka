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
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TaskMetricsTest {

    private final static String THREAD_ID = "test-thread";
    private final static String TASK_ID = "test-task";

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = mock(Sensor.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");


    @Test
    public void shouldGetActiveProcessRatioSensor() {
        final String operation = "active-process-ratio";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.INFO))
                .thenReturn(expectedSensor);

        final String ratioDescription = "The fraction of time the thread spent " +
            "on processing this task among all assigned active tasks";
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            ratioDescription
        );


        final Sensor sensor = TaskMetrics.activeProcessRatioSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetActiveBufferCountSensor() {
        final String operation = "active-buffer-count";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG))
                .thenReturn(expectedSensor);
        final String countDescription = "The count of buffered records that are polled " +
            "from consumer and not yet processed for this active task";
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            countDescription
        );


        final Sensor sensor = TaskMetrics.activeBufferedRecordsSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetProcessLatencySensor() {
        final String operation = "process-latency";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG))
                .thenReturn(expectedSensor);
        final String avgLatencyDescription = "The average latency of calls to process";
        final String maxLatencyDescription = "The maximum latency of calls to process";
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            avgLatencyDescription,
            maxLatencyDescription
        );

        final Sensor sensor = TaskMetrics.processLatencySensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String operation = "punctuate";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG))
                .thenReturn(expectedSensor);
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to punctuate";
        final String rateDescription = "The average number of calls to punctuate per second";
        final String avgLatencyDescription = "The average latency of calls to punctuate";
        final String maxLatencyDescription = "The maximum latency of calls to punctuate";
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription
        );

        final Sensor sensor = TaskMetrics.punctuateSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCommitSensor() {
        final String operation = "commit";
        final String totalDescription = "The total number of calls to commit";
        final String rateDescription = "The average number of calls to commit per second";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).thenReturn(expectedSensor);
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );

        final Sensor sensor = TaskMetrics.commitSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetEnforcedProcessingSensor() {
        final String operation = "enforced-processing";
        final String totalDescription = "The total number of occurrences of enforced-processing operations";
        final String rateDescription = "The average number of occurrences of enforced-processing operations per second";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).thenReturn(expectedSensor);
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );

        final Sensor sensor = TaskMetrics.enforcedProcessingSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetRecordLatenessSensor() {
        final String operation = "record-lateness";
        final String avgDescription =
            "The observed average lateness of records in milliseconds, measured by comparing the record timestamp with "
                + "the current stream time";
        final String maxDescription =
            "The observed maximum lateness of records in milliseconds, measured by comparing the record timestamp with "
                + "the current stream time";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).thenReturn(expectedSensor);
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            avgDescription,
            maxDescription
        );

        final Sensor sensor = TaskMetrics.recordLatenessSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetDroppedRecordsSensor() {
        final String operation = "dropped-records";
        final String totalDescription = "The total number of dropped records";
        final String rateDescription = "The average number of dropped records per second";
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.INFO)).thenReturn(expectedSensor);
        when(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );

        final Sensor sensor = TaskMetrics.droppedRecordsSensor(THREAD_ID, TASK_ID, streamsMetrics);

        assertThat(sensor, is(expectedSensor));
    }
}