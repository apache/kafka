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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class})
public class ProcessorNodeMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String PROCESSOR_NODE_ID = "test-processor";

    private final Sensor expectedSensor = mock(Sensor.class);
    private final Sensor expectedParentSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");
    private final Map<String, String> parentTagMap = Collections.singletonMap("hi", "universe");

    @Before
    public void setUp() {
        expect(streamsMetrics.version()).andStubReturn(Version.LATEST);
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetSuppressionEmitSensor() {
        final String metricNamePrefix = "suppression-emit";
        final String descriptionOfCount = "The total number of emitted records from the suppression buffer";
        final String descriptionOfRate = "The average number of emitted records from the suppression buffer per second";
        expect(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
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
        expect(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
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
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .andReturn(expectedParentSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID))
            .andReturn(parentTagMap);
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

    @Test
    public void shouldGetLateRecordDropSensor() {
        final String metricNamePrefix = "late-record-drop";
        final String descriptionOfCount = "The total number of dropped late records";
        final String descriptionOfRate = "The average number of dropped late records per second";
        setUpThroughputSensor(
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount,
            RecordingLevel.INFO
        );

        verifySensor(() -> ProcessorNodeMetrics.lateRecordDropSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    private void setUpThroughputParentSensor(final String metricNamePrefix,
                                             final String descriptionOfRate,
                                             final String descriptionOfCount) {
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricNamePrefix, RecordingLevel.DEBUG))
            .andReturn(expectedParentSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
            .andReturn(parentTagMap);
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
        expect(streamsMetrics.nodeLevelSensor(
            THREAD_ID,
            TASK_ID,
            PROCESSOR_NODE_ID,
            metricNamePrefix,
            recordingLevel,
            parentSensors
        )).andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
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
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = sensorSupplier.get();

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}