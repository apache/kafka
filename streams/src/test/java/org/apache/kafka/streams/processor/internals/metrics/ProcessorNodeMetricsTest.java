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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProcessorNodeMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String PROCESSOR_NODE_ID = "test-processor";
    private static final String TOPIC_NAME = "topic";

    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");
    private final Map<String, String> parentTagMap = Collections.singletonMap("hi", "universe");

    private final Sensor expectedSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Sensor expectedParentSensor = mock(Sensor.class);

    @Test
    public void shouldGetSuppressionEmitSensor() {
        final String metricNamePrefix = "suppression-emit";
        final String descriptionOfCount = "The total number of emitted records from the suppression buffer";
        final String descriptionOfRate = "The average number of emitted records from the suppression buffer per second";
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
                .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedSensor,
                PROCESSOR_NODE_LEVEL_GROUP,
                tagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
        );

        verifySensor(
                () -> ProcessorNodeMetrics.suppressionEmitSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    @Test
    public void shouldGetIdempotentUpdateSkipSensor() {
        final String metricNamePrefix = "idempotent-update-skip";
        final String descriptionOfCount = "The total number of skipped idempotent updates";
        final String descriptionOfRate = "The average number of skipped idempotent updates per second";
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
                .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedSensor,
                PROCESSOR_NODE_LEVEL_GROUP,
                tagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
        );
        verifySensor(
                () -> ProcessorNodeMetrics.skippedIdempotentUpdatesSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics)
        );
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
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedParentSensor,
                StreamsMetricsImpl.TASK_LEVEL_GROUP,
                parentTagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
        );
        setUpThroughputSensor(
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount,
                RecordingLevel.DEBUG,
                expectedParentSensor
        );

        verifySensor(() -> ProcessorNodeMetrics.processAtSourceSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    @Test
    public void shouldGetRecordsAndBytesConsumedSensor() {
        final String recordsMetricNamePrefix = "records-consumed";
        final String bytesMetricNamePrefix = "bytes-consumed";
        final String descriptionOfRecordsTotal = "The total number of records consumed from this topic";
        final String descriptionOfBytesTotal = "The total number of bytes consumed from this topic";
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, recordsMetricNamePrefix, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, bytesMetricNamePrefix, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);

        final Map<String, String> consumedTagMap = new HashMap<>(tagMap);
        consumedTagMap.put("topic", TOPIC_NAME);
        StreamsMetricsImpl.addTotalCountAndSumMetricsToSensor(
            expectedSensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            consumedTagMap,
            recordsMetricNamePrefix,
            bytesMetricNamePrefix,
            descriptionOfRecordsTotal,
            descriptionOfBytesTotal
        );
        verifySensor(
            () -> ProcessorNodeMetrics.consumedSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetRecordsAndBytesProducedSensor() {
        final String recordsMetricNamePrefix = "records-produced";
        final String bytesMetricNamePrefix = "bytes-produced";
        final String descriptionOfRecordsTotal = "The total number of records produced to this topic";
        final String descriptionOfBytesTotal = "The total number of bytes produced to this topic";

        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, recordsMetricNamePrefix, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, bytesMetricNamePrefix, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).thenReturn(tagMap);

        final Map<String, String> producedTagMap = new HashMap<>(tagMap);
        producedTagMap.put("topic", TOPIC_NAME);
        StreamsMetricsImpl.addTotalCountAndSumMetricsToSensor(
            expectedSensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            producedTagMap,
            recordsMetricNamePrefix,
            bytesMetricNamePrefix,
            descriptionOfRecordsTotal,
            descriptionOfBytesTotal
        );
        verifySensor(
            () -> ProcessorNodeMetrics.producedSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetForwardSensor() {
        final String metricNamePrefix = "forward";
        final String descriptionOfCount = "The total number of calls to forward";
        final String descriptionOfRate = "The average number of calls to forward per second";
        setUpThroughputParentSensor(
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount
        );
        setUpThroughputSensor(
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount,
            RecordingLevel.DEBUG,
            expectedParentSensor
        );

        verifySensor(() -> ProcessorNodeMetrics.forwardSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    private void setUpThroughputParentSensor(final String metricNamePrefix,
                                             final String descriptionOfRate,
                                             final String descriptionOfCount) {
        when(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricNamePrefix, RecordingLevel.DEBUG))
                .thenReturn(expectedParentSensor);
        when(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
                .thenReturn(parentTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedParentSensor,
                PROCESSOR_NODE_LEVEL_GROUP,
                parentTagMap,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
        );
    }

    private void setUpThroughputSensor(final String metricNamePrefix,
                                           final String descriptionOfRate,
                                           final String descriptionOfCount,
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
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount
        );
    }

    private void verifySensor(final Supplier<Sensor> sensorSupplier) {
        final Sensor sensor = sensorSupplier.get();
        assertThat(sensor, is(expectedSensor));
    }
}